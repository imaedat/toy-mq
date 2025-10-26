#include <sys/uio.h>
#include <sysexits.h>

#include <chrono>
#include <cmath>
#include <deque>
#include <map>
#include <set>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "proto.hpp"

#include "config.hpp"
#include "descriptor.hpp"
#include "logger.hpp"
#include "socket.hpp"
#include "thread_pool.hpp"

using namespace std::chrono;
using namespace std::string_literals;
using namespace tbd;

namespace toymq {

namespace {
void join_thread(std::thread& thr)
{
    if (thr.joinable()) {
        thr.join();
    }
}
}  // namespace

class broker;

enum class role : uint8_t
{
    publisher = 1,
    subscriber,
};

/****************************************************************************
 * client
 */
class client
{
  public:
    virtual ~client() noexcept = default;
    void handle(uint32_t events, int max_iters = 0);
    io_socket& socket() noexcept
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

  protected:
    broker& broker_;
    logger& logger_;
    io_socket sock_;
    toymq::role role_;
    std::string name_;

    client(broker& b, logger& l, io_socket& s, toymq::role r) noexcept
        : broker_(b)
        , logger_(l)
        , sock_(std::move(s))
        , role_(r)
        , name_((r == role::publisher ? "pub"s : "sub"s) + "-" + std::to_string(*sock_))
    {
    }

    virtual bool dispatch(const message& msg) = 0;
};

/****************************************************************************
 * publisher
 */
class publisher : public client
{
  public:
    publisher(broker& b, logger& l, io_socket& s)
        : client(b, l, s, role::publisher)
    {
    }

    bool receive_publish(const message& msg);

    bool attach_if_detached()
    {
        bool expected = true;
        return detached_.compare_exchange_strong(expected, false);
    }

  private:
    std::atomic<bool> detached_{false};

    bool dispatch(const message& msg) override;
};

/****************************************************************************
 * subscriber
 */
class subscriber
    : public client
    , public std::enable_shared_from_this<subscriber>
{
  public:
    subscriber(broker& b, logger& l, io_socket& s, std::string_view t)
        : client(b, l, s, role::subscriber)
        , topic_(t)
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

    void push(const std::shared_ptr<message>& msg);
    void flush();

    // reactor or worker
    void ack_arrived(uint64_t msgid)
    {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        unacked_msgs_.erase(msgid);  // O(1)
    }

    // reactor
    std::unordered_set<uint64_t> unacked_msgs()
    {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        return unacked_msgs_;
    }

  private:
    std::string topic_;
    std::mutex mtx_;
    std::deque<std::shared_ptr<message>> drainq_;
    std::unordered_set<uint64_t> unacked_msgs_;
    std::atomic<bool> leaving_{false};

    bool dispatch(const message& msg) override;
};

/****************************************************************************
 * broker
 */
class broker
{
    static constexpr uint16_t DEFAULT_PORT = 55555;
    static constexpr size_t DEFAULT_MAX_MESSAGES = 1048576;
    static constexpr unsigned DEFAULT_HI_WATERMARK = 90;
    static constexpr unsigned DEFAULT_LO_WATERMARK = 50;
    static constexpr unsigned DEFAULT_MESSAGE_TTL = 3600;
    static constexpr int DEFAULT_PUBLISH_MAXITERS = 128;
    static constexpr int DEFAULT_SUBSCRIBE_MAXITERS = 0;
    static constexpr size_t DEFAULT_ROUTERS = 2;
    static constexpr size_t DEFAULT_REACTORS = 2;
    static constexpr size_t DEFAULT_WORKERS = 4;

  public:
    explicit broker(const config& cfg)
        : cfg_(cfg)
        , signalfd_({SIGHUP, SIGINT, SIGQUIT, SIGTERM}, true)
        , srvsock_((uint16_t)cfg_.get<int>("listen_port", DEFAULT_PORT))
        , logger_(cfg_.get<std::string>("log_procname", "broker"),
                  cfg_.get<std::string>("log_filepath", "broker.log"))
        , running_(true)
        , nrouters_(cfg.get<int>("router_threads", DEFAULT_ROUTERS))
        , routers_(nrouters_)
        , nreactors_(cfg_.get<int>("reactor_threads", DEFAULT_REACTORS))
        , reactors_(nreactors_)
        , thrpool_(cfg_.get<int>("worker_threads", DEFAULT_WORKERS))
    {
        logger_.set_level(cfg_.get<std::string>("log_level", "info"));
        start_residents();
        epollfd_.add(*signalfd_);
        epollfd_.add(*srvsock_);
        logger_.info("toy-mq broker start");
    }

    ~broker()
    {
        stop_residents();
        logger_.info("toy-mq broker stop");
    }

    void run()
    {
        while (run_one())
            ;
    }

  private:
    struct unacked_msg
    {
        std::shared_ptr<message> msg;
        std::chrono::system_clock::time_point expiry;
        std::set<std::weak_ptr<subscriber>, std::owner_less<std::weak_ptr<subscriber>>> subscribers;
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
    struct reactor
    {
        std::thread thr;
        tbd::epollfd epollfd;
        tbd::eventfd eventfd;
        std::unordered_map<int, std::weak_ptr<subscriber>> subscribers;
        std::mutex mtx;
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

    config cfg_;
    tbd::signalfd signalfd_;
    tcp_server srvsock_;
    epollfd epollfd_;
    logger logger_;
    std::atomic<bool> running_{false};
    // router
    size_t nrouters_;
    std::vector<router> routers_;
    std::atomic<std::chrono::steady_clock::time_point> sub_last_updated_;
    // ack reactor
    size_t nreactors_;
    std::vector<reactor> reactors_;
    // keeper
    std::thread keeper_;
    tbd::eventfd ev_keeper_;
    // sampler
    std::thread sampler_;
    tbd::eventfd ev_sampler_;
    metrics metrics_;
    // generic worker
    thread_pool thrpool_;

    // { fd => client }
    std::unordered_map<int, std::unique_ptr<publisher>> publishers_;
    std::shared_mutex mtx_pubs_;
    std::unordered_map<int, std::shared_ptr<subscriber>> subscribers_;
    std::shared_mutex mtx_subs_;

    // { msgid => { msg, expiry, subscribers } }
    std::map<uint64_t, unacked_msg> unacked_msgs_;
    std::mutex mtx_unack_;

    // main
    bool run_one()
    {
        const auto pub_max = cfg_.get<int>("publish_maxiters", DEFAULT_PUBLISH_MAXITERS);

        auto events = epollfd_.wait();
        for (const auto& ev : events) {
            if (ev.fd == *signalfd_) {
                if (!handle_signal()) {
                    return false;
                }
            }

            if (ev.fd == *srvsock_) {
                accept_new_client(ev.events);
                continue;
            }

            std::shared_lock<decltype(mtx_pubs_)> lkp(mtx_pubs_);
            auto it = publishers_.find(ev.fd);
            if (it != publishers_.end()) {
                lkp.unlock();
                it->second->handle(ev.events, pub_max);
                continue;
            }
            lkp.unlock();
            logger_.debug("broker: unregistered descriptor %d wakeup. client closed ???", ev.fd);
        }

        return true;
    }

    // main
    bool handle_signal()
    {
        auto sig = signalfd_.get_last_signal();
        logger_.info("broker: receive SIG%s", sigabbrev_np(sig));
        switch (sig) {
        case SIGHUP: {
            // XXX
            config ncfg("sample.config");
            logger_.set_level(ncfg.get<std::string>("log_level", "info"));
            break;
        }

        case SIGINT:
        case SIGQUIT:
        case SIGTERM:
            return false;

        default:
            break;
        }

        return true;
    }

    // main
    void accept_new_client(uint32_t events)
    {
        if (events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
            logger_.error("accept_new_client: server socket error, event bits = %08x", events);
            exit(EX_OSERR);
        }

        auto sock = srvsock_.accept();
        auto sockfd = *sock;
    again:
        auto [msg, ec] = helper::recvmsg(sockfd, true);
        if (ec) {
            logger_.error("accept_new_client: recv error (fd=%d): %s", sockfd,
                          ec.message().c_str());
            if (ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) {
                goto again;
            }
            return;
        }

        switch (msg.command()) {
        case command::publish_oneshot: {
            enqueue_deliver(msg, sockfd);
            // `sock` closed on scope end, return w/o epoll registration
            return;
        }

        case command::publish:
        case command::publish_ack: {
            std::unique_lock<decltype(mtx_pubs_)> lk(mtx_pubs_);
            auto [it, _] =
                publishers_.emplace(sockfd, std::make_unique<publisher>(*this, logger_, sock));
            lk.unlock();
            epollfd_.add(sockfd);
            logger_.info("broker: accept new publisher (fd=%d)", sockfd);
            it->second->receive_publish(msg);
            break;
        }

        case command::subscribe: {
            logger_.info("broker: accept new subscriber [%s] (fd=%d)", msg.topic().data(), sockfd);
            std::unique_lock<decltype(mtx_subs_)> lk(mtx_subs_);
            auto [it, _] = subscribers_.emplace(
                sockfd, std::make_shared<subscriber>(*this, logger_, sock, msg.topic()));
            lk.unlock();
            sub_last_updated_ = steady_clock::now();
            delegate_subscriber(it->second);
            break;
        }

        default:
            break;
        }
        logger_.flush();
    }

    void delegate_subscriber(const std::shared_ptr<subscriber>& sub)
    {
        auto& sock = sub->socket();
        sock.set_nonblock(false);
        auto sockfd = *sock;
        auto& r = reactors_[sockfd % nreactors_];

        std::unique_lock<decltype(r.mtx)> lk(r.mtx);
        auto [it, _] = r.subscribers.emplace(sockfd, sub);
        lk.unlock();

        r.epollfd.add(sockfd);
        thrpool_.submit([this, sub_wp = it->second] { redeliver(sub_wp); });
    }

    // worker
    void redeliver(const std::weak_ptr<subscriber>& sub_wp)
    {
        if (auto sub = sub_wp.lock()) {
            std::lock_guard<decltype(mtx_unack_)> lk(mtx_unack_);
            for (const auto& [_, unacked] : unacked_msgs_) {  // O(n)
                if (sub->match(unacked.msg->topic())) {
                    sub->push(unacked.msg);
                }
            }
        }
    }

    // reactor, worker, keeper
    void try_attach()
    {
        const auto max = cfg_.get<int>("max_messages", DEFAULT_MAX_MESSAGES);
        const auto lo = (unsigned long)cfg_.get<int>("low_watermark", DEFAULT_LO_WATERMARK);
        const auto threshold = (unsigned long)std::ceil(1.0 * max * lo / 100);
        if (metrics_.pending + metrics_.unacked <= threshold) {
            std::shared_lock<decltype(mtx_pubs_)> lk(mtx_pubs_);
            for (auto&& [sockfd, pub] : publishers_) {
                if (pub->attach_if_detached()) {
                    logger_.info("broker: pub-%d attached back!", sockfd);
                    epollfd_.add(sockfd);
                }
            }
        }
    }

    /******************************************************************
     * residents
     */
    void start_residents()
    {
        for (size_t i = 0; i < nrouters_; ++i) {
            routers_[i].i = i;
            routers_[i].thr = std::thread([this, i] { msg_router(i); });
        }
        for (size_t i = 0; i < nreactors_; ++i) {
            reactors_[i].thr = std::thread([this, i] { sub_reactor(i); });
        }
        keeper_ = std::thread([this] { house_keeper(); });
        sampler_ = std::thread([this] { sampler(); });
    }

    void stop_residents()
    {
        running_ = false;
        std::for_each(routers_.begin(), routers_.end(), [](auto&& r) { r.cv.notify_one(); });
        std::for_each(reactors_.begin(), reactors_.end(), [](auto&& r) { r.eventfd.write(); });
        ev_keeper_.write();
        ev_sampler_.write();

        std::for_each(routers_.begin(), routers_.end(), [this](auto&& r) { join_thread(r.thr); });
        std::for_each(reactors_.begin(), reactors_.end(), [this](auto&& r) { join_thread(r.thr); });
        join_thread(keeper_);
        join_thread(sampler_);
    }

    void msg_router(size_t i)
    {
        auto& rt = routers_[i];

        std::unique_lock<std::mutex> lkr(rt.mtx);
        while (true) {
            rt.cv.wait(lkr, [this, &rt] { return !rt.queue.empty() || !running_; });
            if (!running_) {
                break;
            }

            auto msg = std::move(rt.queue.front());
            rt.queue.pop_front();
            lkr.unlock();
            metrics_.pending.fetch_sub(1, std::memory_order_relaxed);

            // subscribers snapshot
            if (rt.snap.empty() || rt.last_snapped_ < sub_last_updated_.load()) {
                std::shared_lock<decltype(mtx_subs_)> lk(mtx_subs_);
                rt.snap.reserve(subscribers_.size());
                for (const auto& [_, sub] : subscribers_) {  // O(n)
                    rt.snap.emplace_back(sub);
                }
                rt.last_snapped_ = steady_clock::now();
            }

            // topic matching
            unacked_msg unacked;
            for (const auto& sub_wp : rt.snap) {
                if (auto sub = sub_wp.lock()) {
                    if (sub->match(msg->topic())) {
                        unacked.subscribers.emplace(sub);  // O(log n)
                    }
                } else {
                    rt.snap.clear();
                    logger_.warn("router-%lu: subscriber already closed before match", i);
                }
            }

            // enqueue to emit
            push_to_subscribers(rt, unacked, msg);

            lkr.lock();
        }
    }

    void push_to_subscribers(router& rt, unacked_msg& unacked, std::shared_ptr<message>& msg)
    {
        const auto ttl = seconds(cfg_.get<int>("message_ttl_sec", DEFAULT_MESSAGE_TTL));
        const size_t max = cfg_.get<int>("max_messages", DEFAULT_MAX_MESSAGES);
        const auto& policy = cfg_.get<std::string>("overflow_policy");

        if (!unacked.subscribers.empty()) {
            metrics_.unacked.fetch_add(1, std::memory_order_relaxed);
            const auto msgid = msg->id();
            unacked.msg = std::move(msg);
            unacked.expiry = system_clock::now() + ttl;
            std::lock_guard<decltype(mtx_unack_)> lku(mtx_unack_);
            logger_.debug("router-%lu: #backlog %lu, enq %lu", rt.i, backlog(), msgid);
            if (policy == "drop" && backlog() >= max) {
                auto it = unacked_msgs_.begin();  // O(1)
                logger_.warn("router-%lu: !!! overflow, drop oldest msg %lu !!!", rt.i, it->first);
                unacked_msgs_.erase(it);  // O(1)
                metrics_.unacked.fetch_sub(1, std::memory_order_relaxed);
                metrics_.dropped.fetch_add(1, std::memory_order_relaxed);
            }
            assert(unacked_msgs_.find(msgid) == unacked_msgs_.end());         // O(log n)
            auto [it, _] = unacked_msgs_.emplace(msgid, std::move(unacked));  // O(log n)
            for (auto&& sub_wp : it->second.subscribers) {
                if (auto sub = sub_wp.lock()) {
                    sub->push(it->second.msg);
                } else {
                    rt.snap.clear();
                    logger_.warn("router-%lu: subscriber already closed before push", rt.i);
                }
            }
        }
    }

    void sub_reactor(size_t i)
    {
        const auto sub_max = cfg_.get<int>("subscribe_maxiters", DEFAULT_SUBSCRIBE_MAXITERS);

        auto& r = reactors_[i];
        r.epollfd.add(r.eventfd);

        while (true) {
            auto events = r.epollfd.wait();
            for (const auto& ev : events) {
                if (ev.fd == *r.eventfd) {
                    return;
                }

                std::unique_lock<decltype(r.mtx)> lk(r.mtx);
                auto it = r.subscribers.find(ev.fd);
                if (it == r.subscribers.end()) {
                    logger_.warn("reactor-%lu: subscriber %d already closed", i, ev.fd);
                    r.epollfd.del(ev.fd);
                    continue;
                }

                auto sub = it->second.lock();
                if (!sub) {
                    logger_.warn("reactor-%lu: subscriber %d already expired", i, ev.fd);
                    std::error_code ec;
                    r.epollfd.del(ev.fd, ec);
                    r.subscribers.erase(ev.fd);
                    continue;
                }

                lk.unlock();
                sub->handle(ev.events, sub_max);
            }
        }
    }

    void house_keeper()
    {
        tbd::poll poller(ev_keeper_);
        while (true) {
            const auto& evs = poller.wait(60 * 1000);
            if (!evs.empty()) {
                return;
            }

            {
                std::unique_lock<decltype(mtx_unack_)> lk(mtx_unack_);
                auto now = system_clock::now();
                auto it = unacked_msgs_.begin();
                while (it != unacked_msgs_.end()) {  // O(n)
                    if (it->second.expiry > now) {
                        break;
                    }
                    logger_.warn("keeper: remove expired unacked msg %lu", it->first);
                    it = unacked_msgs_.erase(it);  // O(1)
                    metrics_.unacked.fetch_sub(1, std::memory_order_relaxed);
                }
                lk.unlock();
                try_attach();
            }
        }
    }

    void sampler()
    {
        tbd::poll poller(ev_sampler_);
        while (true) {
            const auto& evs = poller.wait(1000);
            if (!evs.empty()) {
                return;
            }

            logger_.info(
                "sampler: ingress %lu, egress %lu, ack %lu, rejected %lu, dropped %lu, pending %lu, unacked %lu",
                metrics_.ingress.exchange(0, std::memory_order_relaxed),
                metrics_.egress.exchange(0, std::memory_order_relaxed),
                metrics_.ack.exchange(0, std::memory_order_relaxed),
                metrics_.rejected.load(std::memory_order_relaxed),
                metrics_.dropped.load(std::memory_order_relaxed),
                metrics_.pending.load(std::memory_order_relaxed),
                metrics_.unacked.load(std::memory_order_relaxed));
            logger_.flush();
        }
    }

    uint64_t backlog() const
    {
        return metrics_.pending.load(std::memory_order_relaxed) +
               metrics_.unacked.load(std::memory_order_relaxed);
    }

    /******************************************************************
     * from clients
     */
  public:
    // pub:main, sub:reactor(handle), sub:worker(flush, unsubscribe)
    void close_client(role role, io_socket& sock)
    {
        int sockfd = *sock;

        if (role == role::publisher) {
            std::lock_guard<decltype(mtx_pubs_)> lkp(mtx_pubs_);
            auto it = publishers_.find(sockfd);
            if (it != publishers_.end()) {
                epollfd_.del(sockfd);
                sock.close();
                publishers_.erase(it);
                logger_.info("broker: close publisher (fd=%d)", sockfd);
                logger_.flush();
            }

        } else if (role == role::subscriber) {
            std::lock_guard<decltype(mtx_subs_)> lks(mtx_subs_);
            auto jt = subscribers_.find(sockfd);
            if (jt != subscribers_.end()) {
                subscribers_.erase(sockfd);
                sub_last_updated_ = steady_clock::now();

                auto& r = reactors_[sockfd % nreactors_];
                std::lock_guard<decltype(r.mtx)> lkr(r.mtx);
                std::error_code ec;
                r.epollfd.del(sockfd, ec);
                r.subscribers.erase(sockfd);

                logger_.info("broker: close subscriber (fd=%d)", sockfd);
                logger_.flush();
            }

        } else {
            assert(false);
        }
    }

    /*
     * from publisher
     */
  public:
    // main -> router
    std::pair<bool, uint64_t> enqueue_deliver(const message& orig_msg, int sockfd)
    {
        const auto& policy = cfg_.get<std::string>("overflow_policy");
        if (policy == "reject" && !under_watermark()) {
            return {false, 0};
        }

        metrics_.ingress.fetch_add(1, std::memory_order_relaxed);
        metrics_.pending.fetch_add(1, std::memory_order_relaxed);

        auto msgid = next_id();
        auto new_msg = std::make_shared<message>(orig_msg, msgid);

        auto& rt = routers_[sockfd % nrouters_];
        std::lock_guard<decltype(rt.mtx)> lk(rt.mtx);
        rt.queue.emplace_back(std::move(new_msg));
        rt.cv.notify_one();

        return {true, msgid};
    }

  private:
    // main
    uint64_t next_id() noexcept
    {
        if (cfg_.get<bool>("debug_seqid", false)) {
            static std::atomic<uint64_t> msgid_ = 0;
            return ++msgid_;
        }

        static uint64_t previd_ = 0;
        uint64_t attempts = 0;
    again:
        ++attempts;
        uint64_t newid = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
        if (newid <= previd_) {
            if (attempts >= 100) {
                logger_.warn("broker: cannot generate newid: %lu, %lu", newid, previd_);
                std::this_thread::sleep_for(seconds(1));
            }
            goto again;
        }
        previd_ = newid;
        return newid;
    }

    // main
    bool under_watermark()
    {
        const auto max = cfg_.get<int>("max_messages", DEFAULT_MAX_MESSAGES);
        const auto hi = cfg_.get<int>("high_watermark", DEFAULT_HI_WATERMARK);
        const auto threshold = (unsigned long)std::ceil(1.0 * max * hi / 100);

        bool under = backlog() < threshold;
        if (under) {
            logger_.debug("broker: backlog %lu, threshold %lu", backlog(), threshold);
        } else {
            logger_.warn("broker: !!! backlog over watermark, reject msg !!! (ba %lu, hi %lu)",
                         backlog(), threshold);
            metrics_.rejected.fetch_add(1, std::memory_order_relaxed);
        }
        return under;
    }

  public:
    // main
    void detach_publisher(int sockfd)
    {
        epollfd_.del(sockfd);
    }

    /*
     * from subscriber
     */
  public:
    // reactor
    void receive_ack(std::weak_ptr<subscriber>&& sub_wp, const message& msg)
    {
        metrics_.ack.fetch_add(1, std::memory_order_relaxed);
        auto msgid = msg.id();

        std::unique_lock<decltype(mtx_unack_)> lk(mtx_unack_, std::defer_lock);
        if (lk.try_lock()) {
            // inline path
            collect_unack(std::move(sub_wp), msgid, "reactor");

        } else {
            // later ...
            thrpool_.submit([this, sub_wp = std::move(sub_wp), msgid]() mutable {
                std::lock_guard<decltype(mtx_unack_)> lk(mtx_unack_);
                collect_unack(std::move(sub_wp), msgid, "worker");
            });
        }
    }

  private:
    // reactor or worker
    void collect_unack(std::weak_ptr<subscriber>&& sub_wp, uint64_t msgid, std::string_view caller)
    {
        if (auto sub = sub_wp.lock()) {
            sub->ack_arrived(msgid);
        }

        auto it = unacked_msgs_.find(msgid);  // O(log n)
        if (it == unacked_msgs_.end()) {
            logger_.warn("%s: unacked msg %lu removed ???", caller.data(), msgid);
            return;
        }

        it->second.subscribers.erase(sub_wp);  // O(log n)
        if (it->second.subscribers.empty()) {
            unacked_msgs_.erase(it);  // O(1)
            metrics_.unacked.fetch_sub(1, std::memory_order_relaxed);
            logger_.debug("%s: all subs sent ack for msg %lu", caller.data(), msgid);
            try_attach();
        }
    }

  public:
    // reactor -> worker
    void enqueue_leave(std::weak_ptr<subscriber>&& sub_wp)
    {
        thrpool_.submit([this, sub_wp = std::move(sub_wp)] {
            if (auto sub = sub_wp.lock()) {
                std::lock_guard<decltype(mtx_unack_)> lk(mtx_unack_);
                const auto& msgs = sub->unacked_msgs();
                for (const auto& msgid : msgs) {          // O(n)
                    auto it = unacked_msgs_.find(msgid);  // O(log n)
                    if (it != unacked_msgs_.end()) {
                        it->second.subscribers.erase(sub_wp);  // O(log n)
                        if (it->second.subscribers.empty()) {
                            unacked_msgs_.erase(it);
                            metrics_.unacked.fetch_sub(1, std::memory_order_relaxed);
                            try_attach();
                        }
                    }
                }

                close_client(role::subscriber, sub->socket());
            }
        });
    }

    // router
    void enqueue_flush(std::weak_ptr<subscriber>&& sub_wp)
    {
        thrpool_.submit([this, sub_wp = std::move(sub_wp)] {
            if (auto sub = sub_wp.lock()) {
                sub->flush();
            }
        });
    }

    void on_emit(uint64_t count = 1)
    {
        metrics_.egress.fetch_add(count, std::memory_order_relaxed);
    }
};

/****************************************************************************
 * clients
 */
// pub:main, sub:reactor
void client::handle(uint32_t events, int max_iters)
{
    if (!(events & EPOLLIN) && (events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP))) {
        logger_.error("%s: client socket error, event bits = %08x", name(), events);
        broker_.close_client(role(), sock_);
        return;
    }

    for (int i = 0; (max_iters == 0 || i < max_iters); ++i) {
        auto [msg, ec] = helper::recvmsg(*sock_, false);
        if (ec) {
            if (ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) {
                return;
            }
            if (ec.value() == ENOENT) {
                logger_.info("%s: Connection closed by peer", name());
            } else {
                logger_.error("%s: recv error: %s", name(), ec.message().c_str());
            }
            broker_.close_client(role(), sock_);
            return;
        }

        if (msg.command() == command::close) {
            logger_.info("%s: close", name());
            broker_.close_client(role(), sock_);
            return;
        }

        if (!dispatch(msg)) {
            return;
        }
    }
}

// main
bool publisher::dispatch(const message& msg)
{
    switch (msg.command()) {
    case command::publish:
    case command::publish_ack: {
        return receive_publish(msg);
    }

    default:
        logger_.error("%s: unexpected command [%x]", name(), (uint8_t)msg.command());
        broker_.close_client(role(), sock_);
        return false;
    }

    return true;
}

// main
bool publisher::receive_publish(const message& msg)
{
    bool requires_ack = msg.command() == command::publish_ack;
    logger_.debug("%s: publish%s for [%s]", name(), requires_ack ? " (w/ ack)" : "",
                  msg.topic().data());
    auto [ok, msgid] = broker_.enqueue_deliver(msg, *sock_);
    if (!ok) {
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
        if (ec) {
            logger_.error("%s: send ack error: %s", name(), ec.message().c_str());
            broker_.close_client(role(), sock_);
            return false;
        }
        logger_.debug("%s: send back ack for msg %lu", name(), msgid);
    }

    return true;
}

// reactor
bool subscriber::dispatch(const message& msg)
{
    switch (msg.command()) {
    case command::ack:
        logger_.debug("%s: ack for msg %lu", name(), msg.id());
        broker_.receive_ack(weak_from_this(), msg);
        break;

    case command::unsubscribe:
        logger_.info("%s: unsubscribe", name());
        leaving_ = true;
        broker_.enqueue_leave(weak_from_this());
        return false;

    default:
        logger_.error("%s: unexpected command [%x]", name(), (uint8_t)msg.command());
        broker_.close_client(role(), sock_);
        return false;
    }

    return true;
}

// router
void subscriber::push(const std::shared_ptr<message>& msg)
{
    if (!leaving_) {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        unacked_msgs_.emplace(msg->id());
        drainq_.emplace_back(msg);
        broker_.enqueue_flush(weak_from_this());
    }
}

// worker
void subscriber::flush()
{
    std::lock_guard<decltype(mtx_)> lk(mtx_);
    if (!drainq_.empty() && sock_) {
        auto iovcnt = std::min(drainq_.size(), (size_t)UIO_MAXIOV);
        std::vector<struct iovec> iov(iovcnt);
        auto it = drainq_.begin();
        for (size_t i = 0; i < iovcnt; ++i, ++it) {
            iov[i].iov_base = (*it)->hdr();
            iov[i].iov_len = (*it)->length();
        }
        struct msghdr msg = {};
        msg.msg_iov = iov.data();
        msg.msg_iovlen = iovcnt;

        if (::sendmsg(*sock_, &msg, MSG_NOSIGNAL) < 0) {
            logger_.error("%s: sendmsg error (count %lu): %s", name(), iovcnt, strerror(errno));
            broker_.close_client(role(), sock_);
            return;
        }
        drainq_.erase(drainq_.begin(), it);
        broker_.on_emit(iovcnt);
        logger_.debug("%s: flush %lu msgs", name(), iovcnt);
    }
}

}  // namespace toymq

int main()
{
    config cfg("sample.config");
    toymq::broker brk(cfg);
    brk.run();
}
