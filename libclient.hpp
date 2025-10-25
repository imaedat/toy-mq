#ifndef TOYMQ_LIB_CLIENT_HPP_
#define TOYMQ_LIB_CLIENT_HPP_

#include "proto.hpp"

#include "socket.hpp"

namespace toymq {

class publisher
{
  public:
    publisher(std::string_view ipaddr, uint16_t port)
        : sock_(ipaddr, port)
    {
        sock_.set_nonblock(false);
    }

    bool publish(std::string_view topic, std::string_view msg, bool require_ack = false)
    {
        return publish(topic, msg.data(), msg.size(), require_ack);
    }

    bool publish(std::string_view topic, const void* data, size_t size, bool require_ack)
    {
        sock_.connect();
        if (once_flag_) {
            auto [_, lport] = sock_.local_endpoint();
            printf("publisher: localport = %u\n", lport);
            once_flag_ = false;
        }
        auto c = require_ack ? command::publish_ack : command::publish;
        if (auto ec = helper::sendmsg(*sock_, c, topic, data, size)) {
            throw std::system_error(ec, "sendmsg");
        }
        if (require_ack) {
            auto [msg, ec] = helper::recvmsg(*sock_, true);
            if (ec) {
                throw std::system_error(ec, "recvmsg");
            }
            return msg.command() == command::ack;
        }
        return true;
    }

    void close()
    {
        helper::sendmsg(*sock_, command::close);
    }

  private:
    tbd::tcp_client sock_;
    bool once_flag_ = true;
};

class subscriber
{
    struct message
    {
        std::string topic;
        std::vector<uint8_t> data;
    };

  public:
    subscriber(std::string_view ipaddr, uint16_t port)
        : sock_(ipaddr, port)
    {
        sock_.set_nonblock(false);
    }

    void subscribe(std::string_view topic,
                   const std::function<bool(const subscriber::message& msg)>& callback)
    {
        sock_.connect();
        auto [_, lport] = sock_.local_endpoint();
        printf("subscriber: localport = %u\n", lport);
        helper::sendmsg(*sock_, command::subscribe, topic);

        while (true) {
            auto [msg, ec] = helper::recvmsg(*sock_, true);
            if (ec) {
                printf("subscribe: recvmsg error: %s\n", ec.message().c_str());
                break;
            }
            // printf("send_ack for msg %lu\n", msg.id());
            helper::send_ack(*sock_, msg.id());

            subscriber::message m;
            m.topic = msg.topic();
            m.data = std::move(msg.clone_data());
            if (!callback(m)) {
                break;
            }
        }
    }

    void unsubscribe()
    {
        helper::sendmsg(*sock_, command::unsubscribe);
    }

  private:
    tbd::tcp_client sock_;
};

}  // namespace toymq

#endif
