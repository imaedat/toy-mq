#ifndef TOYMQ_CLIENT_HPP_
#define TOYMQ_CLIENT_HPP_

// XXX "descriptor.hpp"?
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "logger.hpp"
#include "socket.hpp"

#include "broker.hpp"
#include "proto.hpp"

namespace toymq {

/****************************************************************************
 * client
 */
class client
{
    struct socket_closed
    {};

  public:
    virtual ~client() noexcept = default;

    tbd::io_socket& socket() noexcept
    {
        return sock_;
    }

    toymq::role role() const noexcept
    {
        return role_;
    }

    const char* name() const noexcept
    {
        return name_.c_str();
    }

    // pub:main, sub:reactor
    void handle(uint32_t events, int max_iters = 0)
    {
        try {
            if (unlikely(!(events & EPOLLIN) && (events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)))) {
                logger_.error("%s: client socket error, event bits = %08x", name(), events);
                throw socket_closed();
            }

            for (int i = 0; (max_iters == 0 || i < max_iters); ++i) {
                auto [msg, ec] = helper::recvmsg(*sock_, false);
                if (unlikely(ec)) {
                    if (ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) {
                        return;
                    }
                    if (ec.value() == ENOENT) {
                        logger_.info("%s: Connection closed by peer", name());
                    } else {
                        logger_.error("%s: recv error: %s", name(), ec.message().c_str());
                    }
                    throw socket_closed();
                }

                if (unlikely(msg.command() == command::close)) {
                    logger_.info("%s: close", name());
                    throw socket_closed();
                }

                if (unlikely(!dispatch(msg))) {
                    return;
                }
            }

        } catch (const socket_closed&) {
            notify_close();
        }
    }

  protected:
    toymq::broker& broker_;
    tbd::logger& logger_;
    tbd::io_socket sock_;
    const toymq::role role_;
    const std::string name_;

    client(broker& b, tbd::logger& l, tbd::io_socket& s, toymq::role r) noexcept
        : broker_(b)
        , logger_(l)
        , sock_(std::move(s))
        , role_(r)
        , name_((r == role::publisher ? std::string("pub") : std::string("sub")) + "-" +
                std::to_string(*sock_))
    {
    }

    virtual bool dispatch(const message& msg) = 0;
    virtual void notify_close() = 0;
};

/****************************************************************************
 * publisher
 */
class publisher : public client
{
  public:
    publisher(broker& b, tbd::logger& l, tbd::io_socket& s)
        : client(b, l, s, role::publisher)
    {
    }

    // main
    bool receive_publish(const message& msg)
    {
        bool requires_ack = msg.command() == command::publish_ack;
        logger_.trace("%s: publish%s for [%s]", name(), requires_ack ? " (w/ ack)" : "",
                      msg.topic().data());
        auto [ok, msgid] = broker_.enqueue_deliver(msg, *sock_);
        if (unlikely(!ok)) {
            if (requires_ack) {
                helper::sendmsg(*sock_, command::nack);
            }
            logger_.warn("%s: detach for backpressure", name());
            broker_.detach_publisher(*sock_);
            detached_ = true;
            return false;
        }
        if (requires_ack) {
            auto ec = helper::send_ack(*sock_, msgid);
            if (unlikely(ec)) {
                logger_.error("%s: send ack error: %s", name(), ec.message().c_str());
                broker_.close_publisher(sock_);
                return false;
            }
            logger_.debug("%s: send back ack for msg %lu", name(), msgid);
        }

        return true;
    }

    bool attach_if_detached()
    {
        bool expected = true;
        return detached_.compare_exchange_strong(expected, false);
    }

  private:
    std::atomic<bool> detached_{false};

    // main
    bool dispatch(const message& msg) override
    {
        switch (msg.command()) {
        case command::publish:
        case command::publish_ack: {
            return receive_publish(msg);
        }

        default:
            logger_.error("%s: unexpected command [%x]", name(), (uint8_t)msg.command());
            broker_.close_publisher(sock_);
            return false;
        }

        return true;
    }

    void notify_close() override
    {
        broker_.close_publisher(sock_);
    }
};

/****************************************************************************
 * subscriber
 */
class subscriber
    : public client
    , public std::enable_shared_from_this<subscriber>
{
  public:
    subscriber(broker& b, tbd::logger& l, tbd::io_socket& s, std::string_view t,
               std::string_view uuid)
        : client(b, l, s, role::subscriber)
        , topic_(t)
        , client_id_(uuid)
    {
        sock_.set_nonblock(false);

        if (topic_ == "*") {
            topic_.clear();
        } else {
            auto n = topic_.size();
            if (n >= 3 && topic_[n - 2] == '.' && topic_[n - 1] == '*') {
                topic_.erase(n - 1);  // "foo.bar.*" => "foo.bar."
            }
        }
    }

    const std::string& client_id() const noexcept
    {
        return client_id_;
    }

    bool match(std::string_view msg_topic) const
    {
        bool match = false;
        if (topic_.empty()) {
            match = true;
        } else if (topic_.size() > msg_topic.size()) {
            match = false;
        } else {
            auto pos = msg_topic.find(topic_);
            match = (pos == 0 && (msg_topic.size() == topic_.size() || topic_.back() == '.'));
        }

        logger_.debug("%s: my topic [%s], msg topic [%s] => match=%s", name(), topic_.c_str(),
                      msg_topic.data(), match ? "true" : "false");
        return match;
    }

    // reactor or worker
    void ack_arrived(uint64_t msgid)
    {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        unacked_msgids_.erase(msgid);  // O(1)
    }

    // reactor
    std::unordered_set<uint64_t> unacked_msgids()
    {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        return unacked_msgids_;
    }

    // router
    void push(const std::shared_ptr<message>& msg)
    {
        if (unlikely(!leaving_)) {
            std::lock_guard<decltype(mtx_)> lk(mtx_);
            unacked_msgids_.emplace(msg->id());
            drainq_.emplace_back(msg);
            broker_.enqueue_flush(weak_from_this());
        }
    }

    // worker
    void flush()
    {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        if (!drainq_.empty() && !leaving_) {
            auto iovcnt = std::min(drainq_.size(), (size_t)UIO_MAXIOV);
            std::vector<struct iovec> iov(iovcnt);
            std::string ids;
            ids.reserve(256);
            auto it = drainq_.begin();
            for (size_t i = 0; i < iovcnt; ++i, ++it) {
                iov[i].iov_base = (*it)->hdr();
                iov[i].iov_len = (*it)->length();
                ids.append(" ").append(std::to_string((*it)->id()));
            }
            struct msghdr msg = {};
            msg.msg_iov = iov.data();
            msg.msg_iovlen = iovcnt;

            if (unlikely(::sendmsg(*sock_, &msg, MSG_NOSIGNAL) < 0)) {
                logger_.error("%s: sendmsg error (count %lu): %s", name(), iovcnt, strerror(errno));
                leaving_ = true;
                return;
            }
            drainq_.erase(drainq_.begin(), it);
            broker_.on_emit(iovcnt);
            logger_.debug("%s: flush %lu msgs,%s", name(), iovcnt, ids.c_str());
        }
    }

  private:
    std::string topic_;
    std::string client_id_;
    std::mutex mtx_;
    std::deque<std::shared_ptr<message>> drainq_;
    std::unordered_set<uint64_t> unacked_msgids_;
    std::atomic<bool> leaving_{false};

    // reactor
    bool dispatch(const message& msg) override
    {
        try {
            switch (msg.command()) {
            case command::ack:
                logger_.debug("%s: ack for msg %lu", name(), msg.id());
                broker_.receive_ack(weak_from_this(), msg);
                return true;
                break;

            case command::unsubscribe:
                logger_.info("%s: unsubscribe", name());
                leaving_ = true;
                broker_.unsubscribe(weak_from_this());
                return false;

            default:
                logger_.error("%s: unexpected command [%x]", name(), (uint8_t)msg.command());
                broker_.close_subscriber(client_id_);
                return false;
            }

        } catch (const std::exception& e) {
            logger_.error("%s: unexpected error occurred: %s", name(), e.what());
            return false;
        }
    }

    void notify_close() override
    {
        broker_.close_subscriber(client_id_);
    }
};

}  // namespace toymq

#endif
