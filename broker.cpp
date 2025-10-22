#include <sysexits.h>

#include <chrono>
#include <cmath>
#include <deque>
#include <map>
#include <set>
#include <thread>
#include <unordered_map>

#include "proto.hpp"

#include "config.hpp"
#include "descriptor.hpp"
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

#define DEBUG(...) LOG_(verbosity::debug, "DEBUG: " __VA_ARGS__)
#define INFO(...) LOG_(verbosity::info, "INFO: " __VA_ARGS__)
#define WARN(...) LOG_(verbosity::warn, "WARN: " __VA_ARGS__)
#define ERROR(...) LOG_(verbosity::error, "ERROR: " __VA_ARGS__)

class broker;

/****************************************************************************
 * client
 */
class client
{
  public:
    virtual ~client() noexcept = default;
    void handle(uint32_t, int);
    const io_socket& socket() const noexcept
    {
        return sock_;
    }

  protected:
    broker& broker_;
    io_socket sock_;
    std::string role_;
    std::mutex mtx_sock_;

    client(broker& b, io_socket& s, const std::string& role) noexcept
        : broker_(b)
        , sock_(std::move(s))
        , role_(role + "-" + std::to_string(*sock_))
    {
    }

    const char* role() const noexcept
    {
        return role_.c_str();
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
        : client(b, s, "pub")
    {
    }

  private:
    bool dispatch(const message& msg) const override;
};

/****************************************************************************
 * subscriber
 */
class subscriber
    : public client
    , public std::enable_shared_from_this<subscriber>
{
  public:
    subscriber(broker& b, io_socket& s, std::string_view t)
        : client(b, s, "sub")
        , topic_(t)
    {
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

        DEBUG("%s: my topic [%s], msg topic [%s] => match=%s\n", role(), topic_.c_str(),
              msg_topic.data(), match ? "true" : "false");
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
    static constexpr size_t DEFAULT_WORKERS = 4;

  public:
    explicit broker(const config& cfg)
        : cfg_(cfg)
        , srvsock_((uint16_t)cfg_.get<int>("listen_port", DEFAULT_PORT))
        , running_(true)
        , thrpool_(cfg_.get<int>("worker_threads", DEFAULT_WORKERS))
        , nrouters_(cfg.get<int>("router_threads", DEFAULT_ROUTERS))
        , routers_(nrouters_)
    {
        verbosity_ = cfg_.get<int>("verbosity", verbosity::info);
        start_residents();
        epollfd_.add(*srvsock_);
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
        std::set<std::weak_ptr<const subscriber>, std::owner_less<std::weak_ptr<const subscriber>>>
            subscribers;
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
    thread_pool thrpool_;
    // router
    size_t nrouters_;
    std::vector<router> routers_;
    // keeper
    std::thread keeper_;
    tbd::eventfd ev_keeper_;
    // sampler
    std::thread sampler_;
    tbd::eventfd ev_sampler_;
    metrics metrics_;

    // { fd => client }
    std::unordered_map<int, std::unique_ptr<publisher>> publishers_;
    std::unordered_map<int, std::shared_ptr<subscriber>> subscribers_;
    std::mutex mtx_clients_;

    // { msgid => { msg, expiry, subscribers } }
    std::map<uint64_t, unacked_msg> unacked_msgs_;
    std::mutex mtx_unack_;

    // main
    void run_one()
    {
        static const auto pub_max = cfg_.get<int>("publish_maxiters", DEFAULT_PUBLISH_MAXITERS);
        static const auto sub_max = cfg_.get<int>("subscribe_maxiters", DEFAULT_SUBSCRIBE_MAXITERS);

        auto events = epollfd_.wait();
        for (const auto& ev : events) {
            auto fd = ev.fd;
            if (fd == *srvsock_) {
                accept_new_client(ev.events);
                continue;
            }

            // thrpool_.submit([this, fd] {
            std::unique_lock<decltype(mtx_clients_)> lk(mtx_clients_);
            auto it = subscribers_.find(fd);
            if (it != subscribers_.end()) {
                lk.unlock();
                it->second->handle(ev.events, sub_max);
                continue;
            }
            auto jt = publishers_.find(fd);
            if (jt != publishers_.end()) {
                lk.unlock();
                jt->second->handle(ev.events, pub_max);
                continue;
            }
            DEBUG("broker: unregistered descriptor %d wakeup. client closed ???\n", fd);
            // });
        }
    }

    // main
    void accept_new_client(uint32_t events)
    {
        if (events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
            ERROR("accept_new_client: server socket error, event bits = %08x\n", events);
            exit(EX_OSERR);
        }

        auto sock = srvsock_.accept();
        auto sockfd = *sock;
    again:
        auto [msg, ec] = helper::recvmsg(sockfd, true);
        if (ec) {
            ERROR("accept_new_client: recv error (fd=%d): %s\n", sockfd, ec.message().c_str());
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

        case command::publish: {
            INFO("broker: accept new publisher (fd=%d)\n", sockfd);
            {
                std::lock_guard<decltype(mtx_clients_)> lk(mtx_clients_);
                publishers_.emplace(sockfd, std::make_unique<publisher>(*this, sock));
            }
            epollfd_.add(sockfd);
            enqueue_deliver(msg, sockfd);
            break;
        }

        case command::subscribe: {
            INFO("broker: accept new subscriber [%s] (fd=%d)\n", msg.topic().data(), sockfd);
            {
                std::lock_guard<decltype(mtx_clients_)> lk(mtx_clients_);
                subscribers_.emplace(sockfd,
                                     std::make_shared<subscriber>(*this, sock, msg.topic()));
            }
            epollfd_.add(sockfd);
            redeliver(subscribers_[sockfd]);
            break;
        }

        default:
            break;
        }
    }

    void redeliver(std::shared_ptr<subscriber>& sub)
    {
        std::lock_guard<decltype(mtx_unack_)> lk(mtx_unack_);
        for (const auto& [_, unacked] : unacked_msgs_) {  // O(n)
            if (sub->match(unacked.msg->topic())) {
                sub->push(unacked.msg);
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
        ev_keeper_.write();
        ev_sampler_.write();

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
            for (const auto& [_, sub] : req.subscribers) {
                if (sub->match(topic)) {
                    unacked.subscribers.emplace(sub);
                }
            }

            if (!unacked.subscribers.empty()) {
                metrics_.backlog.fetch_add(1, std::memory_order_relaxed);
                unacked.msg = std::move(req.msg);
                unacked.expiry = system_clock::now() + ttl;

                std::lock_guard<decltype(mtx_unack_)> lk2(mtx_unack_);
                DEBUG("router-%lu: #unacked %lu, enq %lu\n", i, unacked_msgs_.size(), req.msgid);
                if (unacked_msgs_.size() >= max) {
                    auto it = unacked_msgs_.begin();  // O(1)
                    WARN("router-%lu: remove oldest unacked msg %lu\n", i, it->first);
                    unacked_msgs_.erase(it);  // O(1)
                }
                assert(unacked_msgs_.find(req.msgid) == unacked_msgs_.end());         // O(log n)
                auto [it, _] = unacked_msgs_.emplace(req.msgid, std::move(unacked));  // O(log n)
                for (auto&& sub : it->second.subscribers) {
                    if (auto sp = sub.lock()) {
                        std::const_pointer_cast<subscriber>(sp)->push(it->second.msg);
                    } else {
                        WARN("router-%lu: subscriber already disconnected before push\n", i);
                    }
                }
            }

            lk.lock();
        }
    }

    void house_keeper()
    {
        tbd::poll poller(ev_keeper_);
        while (true) {
            (void)poller.wait(60 * 1000);
            if (!running_) {
                break;
            }

            {
                std::lock_guard<decltype(mtx_unack_)> lk(mtx_unack_);
                auto now = system_clock::now();
                auto it = unacked_msgs_.begin();
                while (it != unacked_msgs_.end()) {  // O(n)
                    if (it->second.expiry > now) {
                        break;
                    }
                    WARN("keeper: remove expired unacked msg %lu\n", it->first);
                    it = unacked_msgs_.erase(it);  // O(1)
                    metrics_.backlog.fetch_sub(1, std::memory_order_relaxed);
                }
            }
        }
    }

    void sampler()
    {
        tbd::poll poller(ev_sampler_);
        while (true) {
            (void)poller.wait(1000);
            if (!running_) {
                break;
            }

            INFO("sampler: ingress %lu, egress %lu, ack %lu, backlog %lu\n",
                 metrics_.ingress.exchange(0, std::memory_order_relaxed),
                 metrics_.egress.exchange(0, std::memory_order_relaxed),
                 metrics_.ack.exchange(0, std::memory_order_relaxed),
                 metrics_.backlog.load(std::memory_order_relaxed));
        }
    }

    /*
     * from client
     */
  public:
    // main, worker
    void close_client(int sockfd)
    {
        epollfd_.del(sockfd);

        std::lock_guard<decltype(mtx_clients_)> lk(mtx_clients_);
        auto it = publishers_.find(sockfd);
        if (it != publishers_.end()) {
            publishers_.erase(it);
            return;
        }
        subscribers_.erase(sockfd);
        INFO("broker: close client (fd=%d)\n", sockfd);
    }

    /*
     * from publisher
     */
  public:
    // main
    bool enqueue_deliver(const message& orig_msg, int sockfd)
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

        auto& rt = routers_[sockfd % nrouters_];
        std::scoped_lock lk(rt.mtx, mtx_clients_);
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
        std::lock_guard<decltype(mtx_unack_)> lk(mtx_unack_);
        return unacked_msgs_.size() < threshold;
    }

    /*
     * from subscriber
     */
  public:
    // main
    void receive_ack(const std::weak_ptr<const subscriber>& sub, const message& msg)
    {
        auto msgid = msg.id();
        // thrpool_.submit([this, sub, msgid] {
        metrics_.ack.fetch_add(1, std::memory_order_relaxed);

        std::lock_guard<decltype(mtx_unack_)> lk(mtx_unack_);
        auto it = unacked_msgs_.find(msgid);  // O(log n)
        assert(it != unacked_msgs_.end());
        it->second.subscribers.erase(sub);  // O(log n)
        if (it->second.subscribers.empty()) {
            unacked_msgs_.erase(it);  // O(1)
            metrics_.backlog.fetch_sub(1, std::memory_order_relaxed);
            DEBUG("worker: all subs sent ack for msg %lu\n", msgid);
        }
        // });
    }

    // main
    void unsubscribe(const std::weak_ptr<const subscriber>& sub)
    {
        std::lock_guard<decltype(mtx_unack_)> lk(mtx_unack_);
        auto it = unacked_msgs_.begin();
        while (it != unacked_msgs_.end()) {              // O(n)
            auto jt = it->second.subscribers.find(sub);  // O(log n), hmm...
            if (jt != it->second.subscribers.end()) {
                it->second.subscribers.erase(jt);  // O(1)
                if (it->second.subscribers.empty()) {
                    it = unacked_msgs_.erase(it);
                    metrics_.backlog.fetch_sub(1, std::memory_order_relaxed);
                    continue;
                }
            }
            ++it;
        }

        if (auto sp = sub.lock()) {
            close_client(*sp->socket());
        }
    }

    // router
    void enqueue_flush(std::weak_ptr<subscriber> sub)
    {
        thrpool_.submit([this, sub = std::move(sub)] {
            if (auto sp = sub.lock()) {
                sp->flush();
            }
        });
    }

    void increment_egress(uint64_t count = 1)
    {
        metrics_.egress.fetch_add(count, std::memory_order_relaxed);
    }
};

/****************************************************************************
 * clients
 */
// main
void client::handle(uint32_t events, int max_iters)
{
    if (events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
        ERROR("%s: client socket error, event bits = %08x\n", role(), events);
        broker_.close_client(*sock_);
        return;
    }

    for (int i = 0; (max_iters == 0 || i < max_iters); ++i) {
        // XXX
        // std::unique_lock<std::mutex> lk(mtx_sock_);
        auto [msg, ec] = helper::recvmsg(*sock_);
        // lk.unlock();
        if (ec) {
            if (ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) {
                return;
            }
            if (ec.value() == ENOENT) {
                INFO("%s: Connection closed by peer\n", role());
            } else {
                ERROR("%s: recv error: %s\n", role(), ec.message().c_str());
            }
            broker_.close_client(*sock_);
            return;
        }

        if (msg.command() == command::close) {
            INFO("%s: close\n", role());
            broker_.close_client(*sock_);
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
    static message prev_;
    message curr(msg);

    switch (msg.command()) {
    case command::publish:
        DEBUG("%s: publish for [%s]\n", role(), msg.topic().data());
        if (!broker_.enqueue_deliver(msg, *sock_)) {
            return false;
        }
        break;

    default:
        ERROR("%s: unexpected command [%x]\n", role(), (uint8_t)msg.command());
        hexdump((uint8_t*)prev_.hdr() - 16, prev_.length() + 32);
        hexdump((uint8_t*)msg.hdr() - 16, 64);
        broker_.close_client(*sock_);
        return false;
    }

    prev_ = std::move(curr);
    return true;
}

// main
bool subscriber::dispatch(const message& msg) const
{
    switch (msg.command()) {
    case command::ack:
        DEBUG("%s: ack for msg %lu\n", role(), msg.id());
        broker_.receive_ack(weak_from_this(), msg);
        break;

    case command::unsubscribe:
        INFO("%s: unsubscribe\n", role());
        broker_.unsubscribe(weak_from_this());
        return false;

    default:
        ERROR("%s: unexpected command [%x]\n", role(), (uint8_t)msg.command());
        broker_.close_client(*sock_);
        return false;
    }

    return true;
}

// router
void subscriber::push(const std::shared_ptr<message>& msg)
{
    std::lock_guard<decltype(mtx_)> lk(mtx_);
    drainq_.emplace_back(msg);
    broker_.enqueue_flush(weak_from_this());
}

// worker
void subscriber::flush()
{
    std::lock_guard<decltype(mtx_)> lk(mtx_);
    if (!drainq_.empty() && sock_) {
        auto iovcnt = drainq_.size();
        std::vector<struct iovec> iov(iovcnt);
        for (size_t i = 0; i < iovcnt; ++i) {
            iov[i].iov_base = drainq_[i]->hdr();
            iov[i].iov_len = drainq_[i]->length();
        }
        struct msghdr msg = {};
        msg.msg_iov = iov.data();
        msg.msg_iovlen = iovcnt;
    again:
        auto nbytes = ::sendmsg(*sock_, &msg, MSG_NOSIGNAL);
        if (nbytes < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                goto again;
            }
            ERROR("%s: sendmsg error: %s\n", role(), strerror(errno));
            broker_.close_client(*sock_);
            sock_.close();
            return;
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
