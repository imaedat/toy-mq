#ifndef TOYMQ_BROKER_HPP_
#define TOYMQ_BROKER_HPP_

#include <sys/uio.h>
#include <sysexits.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config.hpp"
#include "descriptor.hpp"
#include "logger.hpp"
#include "socket.hpp"
#define THREAD_POOL_ENABLE_WAIT_ALL
#include "thread_pool.hpp"

#include "persistence.hpp"
#include "proto.hpp"

namespace toymq {

namespace {
void join_thread(std::thread& thr)
{
    if (likely(thr.joinable())) {
        thr.join();
    }
}
}  // namespace

/****************************************************************************
 * periodic thread
 */
class periodic_worker
{
  public:
    template <typename F>
    periodic_worker(int interval_ms, F&& task)
        : eventfd_()
        , poller_(eventfd_)
        , thr_([this, interval_ms, task = std::forward<F>(task)] { loop(interval_ms, task); })
    {
    }

    ~periodic_worker()
    {
        stop();
        join_thread(thr_);
    }

    void stop() const
    {
        eventfd_.write();
    }

  private:
    tbd::eventfd eventfd_;
    tbd::poll poller_;
    std::thread thr_;

    template <typename F>
    void loop(int interval_ms, F&& task) const
    {
        using namespace std::chrono;
        int wait_ms = interval_ms;
        while (true) {
            const auto& evs = poller_.wait(wait_ms);
            auto t = steady_clock::now();
            if (unlikely(!evs.empty())) {
                break;
            }

            task();

            auto elapsed_ms = duration_cast<milliseconds>(steady_clock::now() - t).count();
            wait_ms = std::max((int)(interval_ms - elapsed_ms), 0);
        }
    }
};

class reactor;
class publisher;
class subscriber;

/****************************************************************************
 * broker
 */
class broker
{
    struct unacked_msg
    {
        std::shared_ptr<message> msg;
        std::chrono::system_clock::time_point expiry;
        std::unordered_map<std::string, std::weak_ptr<subscriber>> subscribers;
    };
    struct router
    {
        size_t i;
        std::thread thr;
        std::condition_variable cv;
        std::mutex mtx;
        std::deque<std::shared_ptr<message>> queue;
        std::vector<std::weak_ptr<subscriber>> snap;
        std::chrono::steady_clock::time_point last_snapped_;
    };
    struct metrics
    {
        // rate
        std::atomic<uint64_t> ingress{0};
        std::atomic<uint64_t> egress{0};
        std::atomic<uint64_t> ack{0};

        // counter
        std::atomic<uint64_t> rejected{0};
        std::atomic<uint64_t> dropped{0};

        // gauge
        std::atomic<uint64_t> pending{0};
        std::atomic<uint64_t> unacked{0};
    };

    tbd::config cfg_;
    tbd::signalfd signalfd_;
    tbd::tcp_server srvsock_;
    tbd::epollfd epollfd_;
    tbd::logger logger_;
    persistence db_;
    std::atomic<bool> running_{false};

    // { fd => client }
    std::unordered_map<int, std::unique_ptr<publisher>> publishers_;
    std::shared_mutex mtx_pubs_;
    std::unordered_map<std::string, std::pair<size_t, std::weak_ptr<subscriber>>> subscribers_;
    std::shared_mutex mtx_subs_;

    // { msgid => { msg, expiry, subscribers } }
    std::map<uint64_t, unacked_msg> unacked_msgs_;
    std::mutex mtx_unack_;

    // metrics sampler
    metrics metrics_;
    periodic_worker sampler_;
    // house keeper
    periodic_worker keeper_;
    // generic worker
    tbd::thread_pool thrpool_;
    tbd::thread_pool db_writer_;
    // ack reactor
    size_t nreactors_;
    std::vector<std::unique_ptr<reactor>> reactors_;
    // router
    size_t nrouters_;
    std::vector<router> routers_;
    std::atomic<std::chrono::steady_clock::time_point> sub_last_updated_;

  public:
    explicit broker(const tbd::config& cfg);
    ~broker();

    void run();

    // publisher
    std::pair<bool, uint64_t> enqueue_deliver(const message& orig_msg, int sockfd);
    void detach_publisher(int sockfd);
    void close_publisher(tbd::io_socket& sock);

    // subscriber
    void receive_ack(std::weak_ptr<subscriber>&& sub_wp, const message& msg);
    void unsubscribe(std::weak_ptr<subscriber>&& sub_wp);
    void enqueue_flush(std::weak_ptr<subscriber>&& sub_wp);
    void on_emit(uint64_t count = 1);
    void close_subscriber(const std::string& subid);

  private:
    bool run_one();
    bool handle_signal();
    void accept_new_client(uint32_t events);
    void redeliver(const std::weak_ptr<subscriber>& sub_wp);
    void try_attach();

    void start_residents();
    void stop_residents();
    void msg_router(size_t i);
    void push_to_subscribers(router& rt, unacked_msg& unacked);
    void clean_expired_msg();
    void sampling();
    uint64_t backlog() const
    {
        return metrics_.pending.load(std::memory_order_relaxed) +
               metrics_.unacked.load(std::memory_order_relaxed);
    }

    // publisher
    uint64_t next_id() noexcept;
    bool under_watermark(const message& msg);

    // subscriber
    void collect_unack(std::weak_ptr<subscriber>&& sub_wp, uint64_t msgid,
                       std::unique_lock<decltype(mtx_unack_)>& lk, bool fastpath);
    bool remove_unacked(uint64_t msgid, const std::string& subid);
};

}  // namespace toymq
#include "client.hpp"
namespace toymq {

/****************************************************************************
 * ack reactor
 */
class reactor
{
    struct string_like_hash  // {{{
    {
        using is_transparent = void;
        size_t operator()(const std::string& s) const noexcept
        {
            return std::hash<std::string>{}(s);
        }
        size_t operator()(std::string_view sv) const noexcept
        {
            return std::hash<std::string_view>{}(sv);
        }
        size_t operator()(const char* s) const noexcept
        {
            return std::hash<std::string_view>{}(s);
        }
    };

    struct string_like_equal
    {
        using is_transparent = void;
        bool operator()(const std::string& s1, const std::string& s2) const noexcept
        {
            return s1 == s2;
        }
        bool operator()(const std::string& s1, std::string_view s2) const noexcept
        {
            return s1 == s2;
        }
        bool operator()(std::string_view s1, const std::string& s2) const noexcept
        {
            return s1 == s2;
        }
        bool operator()(const std::string& s1, const char* s2) const noexcept
        {
            return s1 == s2;
        }
        bool operator()(const char* s1, const std::string& s2) const noexcept
        {
            return s1 == s2;
        }
    };  // }}}

  public:
    reactor(const tbd::config& c, broker& b, tbd::logger& l, size_t i)
        : cfg_(c)
        , broker_(b)
        , logger_(l)
        , name_(std::string("reactor-") + std::to_string(i))
    {
        epollfd_.add(eventfd_);
        thr_ = std::thread([this] { loop(); });
    }

    ~reactor()
    {
        stop();
        join_thread(thr_);
    }

    void stop() const
    {
        eventfd_.write();
    }

    // main
    std::shared_ptr<subscriber> delegate(tbd::io_socket& sock, const message& msg)
    {
        int sockfd = *sock;
        logger_.debug("%s: delegated subscriber %d", name(), sockfd);
        std::string_view subid((const char*)msg.data());
        std::unique_lock<decltype(mtx_)> lk(mtx_);
        if (unlikely(subscribers_.find(subid.data()) != subscribers_.end())) {
            logger_.error("%s: subscriber id %s is already active, reject it", name(), subid);
            return nullptr;
        }
        auto [it, _] = subscribers_.emplace(
            subid, std::make_shared<subscriber>(broker_, logger_, sock, msg.topic(), subid));
        sock2id_map_.emplace(sockfd, subid);
        lk.unlock();
        epollfd_.add(sockfd);
        return it->second;
    }

    // reactor, worker
    void notify_close(const std::string& subid)
    {
        std::unique_lock<decltype(mtx_)> lk(mtx_);
        auto it = subscribers_.find(subid);
        if (likely(it != subscribers_.end())) {
            int sockfd = *it->second->socket();
            sock2id_map_.erase(sockfd);
            std::error_code ec;
            epollfd_.del(sockfd, ec);
            subscribers_.erase(it);  // dtor -> socket close
        }
        logger_.debug("%s: close subscriber %d", name(), subid);
    }

  private:
    const tbd::config cfg_;
    broker& broker_;
    tbd::logger& logger_;
    std::string name_;
    tbd::epollfd epollfd_;
    tbd::eventfd eventfd_;
    std::unordered_map<std::string, std::shared_ptr<subscriber>> subscribers_;
    std::unordered_map<int, std::string> sock2id_map_;
    std::thread thr_;
    std::mutex mtx_;

    const char* name() const noexcept
    {
        return name_.c_str();
    }

    void loop()
    {
        using namespace std::chrono;
        while (true) {
            auto events = epollfd_.wait();
            for (const auto& ev : events) {
                if (unlikely(ev.fd == *eventfd_)) {
                    return;
                }

                std::unique_lock<decltype(mtx_)> lk(mtx_);
                auto it = sock2id_map_.find(ev.fd);
                if (it != sock2id_map_.end()) {
                    auto jt = subscribers_.find(it->second);
                    if (jt != subscribers_.end()) {
                        lk.unlock();
                        jt->second->handle(ev.events);
                        continue;
                    }
                }

                logger_.warn("%s: subscriber %d already unregistered", name(), ev.fd);
                std::error_code ec;
                epollfd_.del(ev.fd, ec);
            }
        }
    }
};

}  // namespace toymq

#endif

// vim: set foldmethod=marker:
