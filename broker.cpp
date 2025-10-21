#include <chrono>
#include <cmath>
#include <deque>
#include <map>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "proto.hpp"

#include "config.hpp"
#include "fdutils.hpp"
#include "socket.hpp"
#include "thread_pool.hpp"

using namespace tbd;
using namespace std::chrono;

namespace toymq {

enum verbosity
{
    quiet = 0,
    error,
    warn,
    info,
    debug,
};
int verbosity_ = verbosity::info;

#define LOG_(lv, ...)                                                                              \
    do {                                                                                           \
        if (verbosity_ >= (lv)) {                                                                  \
            printf(__VA_ARGS__);                                                                   \
        }                                                                                          \
    } while (0)

#define DEBUG(...) LOG_(verbosity::debug, __VA_ARGS__)
#define INFO(...) LOG_(verbosity::info, __VA_ARGS__)
#define WARN(...) LOG_(verbosity::warn, __VA_ARGS__)
#define ERROR(...) LOG_(verbosity::error, __VA_ARGS__)

class broker;

/****************************************************************************
 * clients
 */
class client
{
  public:
    virtual ~client() noexcept = default;
    void handle(int);

  protected:
    broker& broker_;
    io_socket sock_;
    std::string role_;
    std::mutex mtx_sock_;

    client(broker& b, io_socket& s) noexcept
        : broker_(b)
        , sock_(std::move(s))
    {
    }

    const std::string& role() const noexcept
    {
        return role_;
    }

    virtual bool dispatch(const message& msg) const = 0;
};

/****************************************************************************
 * publisher
 */
class publisher : public client
{
  public:
    publisher(broker& b, io_socket& s)
        : client(b, s)
    {
        role_ = "pub";
    }

  private:
    bool dispatch(const message& msg) const override;
};

/****************************************************************************
 * subscriber
 */
class subscriber : public client
{
  public:
    subscriber(broker& b, io_socket& s, std::string_view t)
        : client(b, s)
        , topic_(t)
    {
        role_ = "sub";

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

        DEBUG("sub-%d: my topic [%s], msg topic [%s] => match=%s\n", sock_.native_handle(),
              topic_.c_str(), msg_topic.data(), match ? "true" : "false");
        return match;
    }

    void push(const std::shared_ptr<message>& msg);
    void flush();

  private:
    std::string topic_;
    std::mutex mtx_;
    std::deque<std::shared_ptr<message>> drainq_;

    bool dispatch(const message& msg) const override;
};

/****************************************************************************
 * broker
 */
class broker
{
    static constexpr uint16_t DEFAULT_PORT = 5555;
    static constexpr size_t DEFAULT_MAX_MESSAGES = 1024;
    static constexpr unsigned DEFAULT_WATERMARK = 80;
    static constexpr unsigned DEFAULT_MESSAGE_TTL = 3600;
    static constexpr int DEFAULT_PUBLISH_MAXITERS = 128;
    static constexpr int DEFAULT_SUBSCRIBE_MAXITERS = 0;
    static constexpr size_t DEFAULT_ROUTERS = 2;
    static constexpr size_t DEFAULT_FLUSHERS = 2;

  public:
    explicit broker(const config& cfg)
        : cfg_(cfg)
        , srvsock_((uint16_t)cfg_.get<int>("listen_port", DEFAULT_PORT))
        , running_(true)
        , nrouters_(cfg.get<int>("router_threads", DEFAULT_ROUTERS))
        , routers_(nrouters_)
        , flushers_(cfg_.get<int>("flusher_threads", DEFAULT_FLUSHERS))
    {
        verbosity_ = cfg_.get<int>("verbosity", verbosity::info);
        start_residents();
        epollfd_.add(srvsock_.native_handle());
    }

    ~broker()
    {
        stop_residents();
    }

    void run()
    {
        while (true) {
            run_one();
        }
    }

  private:
    struct unacked_msg
    {
        std::shared_ptr<message> msg;
        std::chrono::system_clock::time_point expiry;
        std::unordered_set<const subscriber*> subscribers;
    };
    struct delivery_req
    {
        uint64_t msgid;
        std::shared_ptr<message> msg;
        std::unordered_map<int, std::shared_ptr<subscriber>> subscribers;

        delivery_req(uint64_t id, std::shared_ptr<message>& m,
                     const std::unordered_map<int, std::shared_ptr<subscriber>>& ss)
            : msgid(id)
            , msg(std::move(m))
            , subscribers(ss)  // snapshot
        {
        }
    };
    struct router
    {
        std::thread thr;
        std::condition_variable cv;
        std::mutex mtx;
        std::deque<delivery_req> queue;
    };
    struct metrics
    {
        std::atomic<uint64_t> backlog{0};
        std::atomic<uint64_t> ingress{0};
        std::atomic<uint64_t> egress{0};
        std::atomic<uint64_t> ack{0};
    };

    config cfg_;
    tcp_server srvsock_;
    epollfd epollfd_;
    std::atomic<bool> running_{false};
    // router
    size_t nrouters_;
    std::vector<router> routers_;
    thread_pool flushers_;
    // keeper
    std::thread keeper_;
    tbd::eventfd ev_keeper_;
    // sampler
    std::thread sampler_;
    tbd::eventfd ev_sampler_;
    metrics metrics_;
    // XXX
    // thread_pool reactors_{4};

    // { fd => client }
    std::unordered_map<int, std::unique_ptr<publisher>> publishers_;
    std::unordered_map<int, std::shared_ptr<subscriber>> subscribers_;

    // { msgid => { msg, expiry, subscribers } }
    std::map<uint64_t, unacked_msg> unacked_msgs_;
    std::mutex mtx_;

    // main
    void run_one()
    {
        static const auto pub_max = cfg_.get<int>("publish_maxiters", DEFAULT_PUBLISH_MAXITERS);
        static const auto sub_max = cfg_.get<int>("subscribe_maxiters", DEFAULT_SUBSCRIBE_MAXITERS);

        auto events = epollfd_.wait();
        for (const auto& ev : events) {
            auto fd = ev.fd;
            if (fd == srvsock_.native_handle()) {
                accept_new_client();
                continue;
            }

            // reactors_.submit([this, fd] {
            auto it = subscribers_.find(fd);
            if (it != subscribers_.end()) {
                it->second->handle(sub_max);
            } else {
                publishers_[fd]->handle(pub_max);
            }
            // });
        }
    }

    // main
    void accept_new_client()
    {
        auto sock = srvsock_.accept();
        // sock.set_nonblock(false);
        int fd = sock.native_handle();
    again:
        auto [msg, ec] = helper::recvmsg(sock.native_handle());
        if (ec) {
            ERROR("accept_new_client: recv error (fd=%d): %s\n", sock.native_handle(),
                  ec.message().c_str());
            if (ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) {
                goto again;
            }
            return;
        }

        switch (msg.command()) {
        case command::publish_oneshot: {
            enqueue_delivery(msg, fd);
            // `sock` closed on scope end, return w/o epoll registration
            return;
        }

        case command::publish: {
            INFO("broker: accept new publisher (fd=%d)\n", fd);
            publishers_.emplace(fd, std::make_unique<publisher>(*this, sock));
            epollfd_.add(fd);
            enqueue_delivery(msg, fd);
            break;
        }

        case command::subscribe: {
            INFO("broker: accept new subscriber (fd=%d)\n", fd);
            subscribers_.emplace(fd, std::make_shared<subscriber>(*this, sock, msg.topic()));
            epollfd_.add(fd);
            redeliver(subscribers_[fd].get());
            break;
        }

        default:
            break;
        }
    }

    void redeliver(subscriber* sub)
    {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        for (const auto& p : unacked_msgs_) {  // O(n)
            const auto& msg = p.second.msg;
            if (sub->match(msg->topic())) {
                sub->push(msg);
            }
        }
    }

    /******************************************************************
     * residents
     */
    void start_residents()
    {
        for (size_t i = 0; i < nrouters_; ++i) {
            routers_[i].thr = std::thread([this, i] { msg_router(i); });
        }
        keeper_ = std::thread([this] { house_keeper(); });
        sampler_ = std::thread([this] { sampler(); });
    }

    void stop_residents()
    {
        running_ = false;
        for (auto&& r : routers_) {
            r.cv.notify_one();
        }
        ev_keeper_.write(1);
        ev_sampler_.write(1);

        for (auto&& r : routers_) {
            if (r.thr.joinable()) {
                r.thr.join();
            }
        }
        if (keeper_.joinable()) {
            keeper_.join();
        }
        if (sampler_.joinable()) {
            sampler_.join();
        }
    }

    void msg_router(size_t i)
    {
        const auto ttl = seconds(cfg_.get<int>("message_ttl_sec", DEFAULT_MESSAGE_TTL));
        const size_t max = cfg_.get<int>("max_messages", DEFAULT_MAX_MESSAGES);
        auto& rt = routers_[i];

        std::unique_lock<std::mutex> lk(rt.mtx);
        while (true) {
            bool enq = rt.cv.wait_for(lk, seconds(10),
                                      [this, &rt] { return !rt.queue.empty() || !running_; });
            if (!running_) {
                break;
            }
            if (!enq) {
                continue;
            }

            auto req = std::move(rt.queue.front());
            rt.queue.pop_front();
            lk.unlock();

            const auto& topic = req.msg->topic();
            unacked_msg unacked;
            for (const auto& [fd, sub] : req.subscribers) {
                if (sub->match(topic)) {
                    unacked.subscribers.emplace(sub.get());
                }
            }

            if (!unacked.subscribers.empty()) {
                metrics_.backlog.fetch_add(1, std::memory_order_relaxed);
                unacked.msg = std::move(req.msg);
                unacked.expiry = system_clock::now() + ttl;

                std::lock_guard<decltype(mtx_)> lk2(mtx_);
                DEBUG("router-%lu: #unacked %lu, enq %lu\n", i, unacked_msgs_.size(), req.msgid);
                if (unacked_msgs_.size() >= max) {
                    unacked_msgs_.erase(unacked_msgs_.begin());  // O(log n)
                }
                assert(unacked_msgs_.find(req.msgid) == unacked_msgs_.end());
                auto [it, _] = unacked_msgs_.emplace(req.msgid, std::move(unacked));  // O(log n)
                for (auto&& sub : it->second.subscribers) {
                    (const_cast<subscriber*>(sub))->push(it->second.msg);
                }
            }

            lk.lock();
        }
    }

    void house_keeper()
    {
        tbd::poll poller(*ev_keeper_);
        while (true) {
            (void)poller.wait(60 * 1000);
            if (!running_) {
                break;
            }

            {
                std::lock_guard<decltype(mtx_)> lk(mtx_);
                auto now = system_clock::now();
                auto it = unacked_msgs_.begin();
                while (it != unacked_msgs_.end()) {  // O(n)
                    if (it->second.expiry > now) {
                        break;
                    }
                    WARN("keeper: erase msgid %lu\n", it->first);
                    it = unacked_msgs_.erase(it);  // O(1)
                    metrics_.backlog.fetch_sub(1, std::memory_order_relaxed);
                }
            }
        }
    }

    void sampler()
    {
        tbd::poll poller(*ev_sampler_);
        while (true) {
            (void)poller.wait(1000);
            if (!running_) {
                break;
            }

            auto backlog = metrics_.backlog.load(std::memory_order_relaxed);
            auto ingress = metrics_.ingress.exchange(0, std::memory_order_relaxed);
            auto egress = metrics_.egress.exchange(0, std::memory_order_relaxed);
            auto ack = metrics_.ack.exchange(0, std::memory_order_relaxed);

            INFO("sampler: ingress %lu, egress %lu, ack %lu, backlog %lu\n", ingress, egress, ack,
                 backlog);
        }
    }

    /******************************************************************
     * from client
     */
  public:
    // main
    void close_client(int fd)
    {
        epollfd_.del(fd);
        auto it = publishers_.find(fd);
        if (it != publishers_.end()) {
            publishers_.erase(it);
            return;
        }
        subscribers_.erase(fd);
    }

    /******************************************************************
     * from publisher
     */
  public:
    // main
    bool enqueue_delivery(const message& orig_msg, int fd)
    {
        metrics_.ingress.fetch_add(1, std::memory_order_relaxed);

        if (!under_watermark()) {
            WARN("broker: !!! unacked msgs over watermark, reject new msg !!!\n");
            return false;
        }

        auto msgid = next_id();
        auto new_msg = std::make_shared<message>(orig_msg);  // copy
        new_msg->resize(new_msg->length() + sizeof(msgid));
        new_msg->id(msgid);

        auto& rt = routers_[fd % nrouters_];
        std::lock_guard<std::mutex> lk(rt.mtx);
        rt.queue.emplace_back(msgid, new_msg, subscribers_);
        rt.cv.notify_one();

        return true;
    }

  private:
    // main
    uint64_t next_id() const noexcept
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
                WARN("broker: cannot generate newid: %lu, %lu\n", newid, previd_);
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
        const auto wm = cfg_.get<int>("watermark_percent", DEFAULT_WATERMARK);
        const auto threshold = (unsigned)std::ceil(1.0 * max * wm / 100);
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        return unacked_msgs_.size() < threshold;
    }

    /******************************************************************
     * from subscriber
     */
  public:
    // main
    void receive_ack(const subscriber* sub, uint64_t msgid)
    {
        metrics_.ack.fetch_add(1, std::memory_order_relaxed);

        std::lock_guard<decltype(mtx_)> lk(mtx_);
        auto it = unacked_msgs_.find(msgid);  // O(log n)
        assert(it != unacked_msgs_.end());
        it->second.subscribers.erase(sub);  // O(1)
        if (it->second.subscribers.empty()) {
            unacked_msgs_.erase(it);  // O(1)
            metrics_.backlog.fetch_sub(1, std::memory_order_relaxed);
            DEBUG("broker: all subs sent ack for msg %lu\n", msgid);
        }
    }

    // main
    void unsubscribe(const subscriber* sub, int fd)
    {
        close_client(fd);

        std::lock_guard<decltype(mtx_)> lk(mtx_);
        auto it = unacked_msgs_.begin();
        while (it != unacked_msgs_.end()) {  // O(n)
            auto& subs = it->second.subscribers;
            auto jt = subs.find(sub);  // O(1)
            if (jt != subs.end()) {
                subs.erase(jt);
                if (subs.empty()) {
                    metrics_.backlog.fetch_sub(1, std::memory_order_relaxed);
                    it = unacked_msgs_.erase(it);  // O(1)
                    continue;
                }
            }
            ++it;
        }
    }

    // router
    void enqueue_flush(subscriber* sub)
    {
        flushers_.submit([this, sub] { sub->flush(); });
    }

    void increment_egress(uint64_t n = 1)
    {
        metrics_.egress.fetch_add(n, std::memory_order_relaxed);
    }
};

/****************************************************************************
 * clients
 */
// main
void client::handle(int max_iters)
{
    for (int i = 0; (max_iters == 0 || i < max_iters); ++i) {
        // XXX
        // std::unique_lock<std::mutex> lk(mtx_sock_);
        auto [msg, ec] = helper::recvmsg(sock_.native_handle());
        // lk.unlock();
        if (ec) {
            if (ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) {
                return;
            }
            if (ec.value() == ENOENT) {
                INFO("%s-%d: connection closed by peer\n", role().c_str(), sock_.native_handle());
            } else {
                ERROR("%s-%d: recv error: %s\n", role().c_str(), sock_.native_handle(),
                      ec.message().c_str());
            }
            broker_.close_client(sock_.native_handle());
            return;
        }

        if (msg.command() == command::close) {
            INFO("%s-%d: close\n", role().c_str(), sock_.native_handle());
            broker_.close_client(sock_.native_handle());
            return;
        }

        if (!dispatch(msg)) {
            return;
        }
    }
}

// main
bool publisher::dispatch(const message& msg) const
{
    switch (msg.command()) {
    case command::publish:
        DEBUG("pub-%d: publish for [%s]\n", sock_.native_handle(), msg.topic().data());
        if (!broker_.enqueue_delivery(msg, sock_.native_handle())) {
            return false;
        }
        break;

    default:
        ERROR("pub-%d: unexpected command [%x]\n", sock_.native_handle(), (uint8_t)msg.command());
        broker_.close_client(sock_.native_handle());
        return false;
    }

    return true;
}

// main
bool subscriber::dispatch(const message& msg) const
{
    switch (msg.command()) {
    case command::ack:
        DEBUG("sub-%d: ack for msg %lu\n", sock_.native_handle(), msg.id());
        broker_.receive_ack(this, msg.id());
        break;

    case command::unsubscribe:
        INFO("sub-%d: unsubscribe\n", sock_.native_handle());
        broker_.unsubscribe(this, sock_.native_handle());
        return false;

    default:
        ERROR("sub-%d: unexpected command [%x]\n", sock_.native_handle(), (uint8_t)msg.command());
        broker_.close_client(sock_.native_handle());
        return false;
    }

    return true;
}

// router
void subscriber::push(const std::shared_ptr<message>& msg)
{
    std::lock_guard<decltype(mtx_)> lk(mtx_);
    drainq_.emplace_back(msg);
    broker_.enqueue_flush(this);
}

// thread pool
void subscriber::flush()
{
    std::lock_guard<decltype(mtx_)> lk(mtx_);
    if (!drainq_.empty()) {
        auto iovcnt = drainq_.size();
        struct iovec iov[iovcnt];
        for (size_t i = 0; i < iovcnt; ++i) {
            iov[i].iov_base = drainq_[i]->hdr();
            iov[i].iov_len = drainq_[i]->length();
        }
        struct msghdr msg = {};
        msg.msg_iov = iov;
        msg.msg_iovlen = iovcnt;
    again:
        auto nbytes = ::sendmsg(sock_.native_handle(), &msg, MSG_NOSIGNAL);
        if (nbytes < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                goto again;
            } else {
                ERROR("pool: !!! sendmsg error !!!: %s\n", strerror(errno));
            }
        }
        drainq_.clear();
        broker_.increment_egress(iovcnt);
    }
}

}  // namespace toymq

int main()
{
    config cfg("sample.config");
    toymq::broker brk(cfg);
    brk.run();
}
