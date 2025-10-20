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

#undef BROKER_VERBOSE

using namespace tbd;
using namespace std::chrono;

namespace toymq {

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

#ifdef BROKER_VERBOSE
        printf("sub-%d: my topic [%s], msg topic [%s] => match=%s\n", sock_.native_handle(),
               topic_.c_str(), msg_topic.data(), match ? "true" : "false");
#endif
        return match;
    }

    void push(const std::shared_ptr<message>& msg);
    void drain_messages();

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
    static constexpr size_t DEFAULT_WORKERS = 2;
    static constexpr size_t DEFAULT_MAX_MESSAGES = 1024;
    static constexpr unsigned DEFAULT_WATERMARK = 80;
    static constexpr unsigned DEFAULT_MESSAGE_TTL = 3600;
    static constexpr int DEFAULT_PUBLISH_MAXITERS = 128;
    static constexpr int DEFAULT_SUBSCRIBE_MAXITERS = 0;

  public:
    explicit broker(const config& cfg)
        : cfg_(cfg)
        , srvsock_((uint16_t)cfg_.get<int>("listen_port", DEFAULT_PORT))
        , thpool_(cfg_.get<int>("worker_threads", DEFAULT_WORKERS))
        , running_(true)
    {
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
    struct routing_req
    {
        uint64_t msgid;
        std::shared_ptr<message> msg;
        std::unordered_map<int, std::shared_ptr<subscriber>> subscribers;

        routing_req(uint64_t id, std::shared_ptr<message>& m,
                    const std::unordered_map<int, std::shared_ptr<subscriber>>& ss)
            : msgid(id)
            , msg(std::move(m))
            , subscribers(ss)  // snapshot
        {
        }
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
    thread_pool thpool_;
    std::mutex mtx_;
    epollfd epollfd_;
    bool running_;
    // router
    std::thread router_;
    std::condition_variable cv_router_;
    std::deque<routing_req> deliveryq_;
    // keeper
    std::thread keeper_;
    std::condition_variable cv_keeper_;
    // sampler
    std::thread sampler_;
    std::condition_variable cv_sampler_;
    metrics metrics_;

    // { fd => client }
    std::unordered_map<int, std::unique_ptr<publisher>> publishers_;
    std::unordered_map<int, std::shared_ptr<subscriber>> subscribers_;

    // { msgid => { msg, expiry, subscribers } }
    std::map<uint64_t, unacked_msg> unacked_msgs_;

    void run_one()
    {
        const auto pub_max = cfg_.get<int>("publish_maxiters", DEFAULT_PUBLISH_MAXITERS);
        const auto sub_max = cfg_.get<int>("subscribe_maxiters", DEFAULT_SUBSCRIBE_MAXITERS);

        auto events = epollfd_.wait();
        for (const auto& ev : events) {
            if (ev.fd == srvsock_.native_handle()) {
                accept_new_client();
                continue;
            }

            auto it = subscribers_.find(ev.fd);
            if (it != subscribers_.end()) {
                it->second->handle(sub_max);
            } else {
                publishers_[ev.fd]->handle(pub_max);
            }
        }
    }

    void accept_new_client()
    {
        auto sock = srvsock_.accept();
        // sock.set_nonblock(false);
        int fd = sock.native_handle();
        auto [msg, ec] = helper::recvmsg(sock.native_handle());
        if (ec) {
            printf("accept_new_client: recv error (fd=%d): %s\n", sock.native_handle(),
                   ec.message().c_str());
            return;
        }

        switch (msg.command()) {
        case command::publish_oneshot: {
            enqueue_delivery(msg);
            // `sock` closed on scope end, return w/o epoll registration
            return;
        }

        case command::publish: {
            printf("broker: accept new publisher (fd=%d)\n", fd);
            publishers_.emplace(fd, std::make_unique<publisher>(*this, sock));
            epollfd_.add(fd);
            enqueue_delivery(msg);
            break;
        }

        case command::subscribe: {
            printf("broker: accept new subscriber (fd=%d)\n", fd);
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
        router_ = std::thread([this] { msg_router(); });
        keeper_ = std::thread([this] { house_keeper(); });
        sampler_ = std::thread([this] { sampler(); });
    }

    void stop_residents()
    {
        {
            std::lock_guard<decltype(mtx_)> lk(mtx_);
            running_ = false;
            cv_router_.notify_one();
            cv_keeper_.notify_one();
            cv_sampler_.notify_one();
        }
        if (router_.joinable()) {
            router_.join();
        }
        if (keeper_.joinable()) {
            keeper_.join();
        }
        if (sampler_.joinable()) {
            sampler_.join();
        }
    }

    void msg_router()
    {
        const auto ttl = seconds(cfg_.get<int>("message_ttl_sec", DEFAULT_MESSAGE_TTL));
        const size_t max = cfg_.get<int>("max_messages", DEFAULT_MAX_MESSAGES);

        std::unique_lock<decltype(mtx_)> lk(mtx_);
        while (true) {
            bool enq = cv_router_.wait_for(lk, seconds(10),
                                           [this] { return !deliveryq_.empty() || !running_; });
            if (!running_) {
                break;
            }
            if (!enq) {
                continue;
            }

            auto req = std::move(deliveryq_.front());
            deliveryq_.pop_front();
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

                std::lock_guard<decltype(mtx_)> lk(mtx_);
#ifdef BROKER_VERBOSE
                printf("broker: curr unacked = %lu, enq %lu\n", unacked_msgs_.size(), req.msgid);
#endif
                if (unacked_msgs_.size() >= max) {
                    unacked_msgs_.erase(unacked_msgs_.begin());  // O(log n)
                }
                assert(unacked_msgs_.find(req.msgid) == unacked_msgs_.end());
                unacked_msgs_.emplace(req.msgid, std::move(unacked));  // O(log n)
                auto it = unacked_msgs_.rbegin();                      // O(1)
                for (auto&& sub : it->second.subscribers) {
                    (const_cast<subscriber*>(sub))->push(it->second.msg);
                }
            }

            lk.lock();
        }
    }

    void house_keeper()
    {
        std::unique_lock<decltype(mtx_)> lk(mtx_);
        while (true) {
            cv_keeper_.wait_for(lk, minutes(1), [this] { return !running_; });
            if (!running_) {
                break;
            }

            auto now = system_clock::now();
            auto it = unacked_msgs_.begin();
            while (it != unacked_msgs_.end()) {  // O(n)
                if (it->second.expiry > now) {
                    break;
                }
                printf("keeper: erase msgid %lu\n", it->first);
                it = unacked_msgs_.erase(it);  // O(1)
                metrics_.backlog.fetch_sub(1, std::memory_order_relaxed);
            }
        }
    }

    void sampler()
    {
        std::unique_lock<decltype(mtx_)> lk(mtx_);
        while (true) {
            cv_sampler_.wait_for(lk, seconds(1), [this] { return !running_; });
            if (!running_) {
                break;
            }

            auto backlog = metrics_.backlog.load(std::memory_order_relaxed);
            auto ingress = metrics_.ingress.exchange(0, std::memory_order_relaxed);
            auto egress = metrics_.egress.exchange(0, std::memory_order_relaxed);
            auto ack = metrics_.ack.exchange(0, std::memory_order_relaxed);

            printf("sampler: ingress %lu, egress %lu, ack %lu, backlog %lu\n", ingress, egress, ack,
                   backlog);
        }
    }

    /******************************************************************
     * from client
     */
  public:
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
    bool enqueue_delivery(const message& orig_msg)
    {
        metrics_.ingress.fetch_add(1, std::memory_order_relaxed);

        if (!under_watermark()) {
            printf("broker: !!! unacked msgs over watermark, reject new msg !!!\n");
            return false;
        }

        auto msgid = next_id();
        auto new_msg = std::make_shared<message>(orig_msg);  // copy
        new_msg->resize(new_msg->length() + sizeof(msgid));
        new_msg->id(msgid);

        std::lock_guard<decltype(mtx_)> lk(mtx_);
        deliveryq_.emplace_back(msgid, new_msg, subscribers_);
        cv_router_.notify_one();

        return true;
    }

  private:
    uint64_t next_id() const noexcept
    {
        if (cfg_.get<bool>("debug_seqid", false)) {
            static uint64_t msgid_ = 0;
            return ++msgid_;
        }

        static uint64_t previd_ = 0;
        uint64_t attempts = 0;
    again:
        ++attempts;
        uint64_t newid = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
        if (newid <= previd_) {
            if (attempts >= 100) {
                printf("broker: cannot generate newid: %lu, %lu\n", newid, previd_);
                std::this_thread::sleep_for(seconds(1));
            }
            goto again;
        }
        previd_ = newid;
        return newid;
    }

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
#ifdef BROKER_VERBOSE
            printf("broker: all subs sent ack for msg %lu\n", msgid);
#endif
        }
    }

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

    void drain(subscriber* sub)
    {
        thpool_.submit([this, sub] { sub->drain_messages(); });
    }

    void increment_egress(uint64_t n = 1)
    {
        metrics_.egress.fetch_add(n, std::memory_order_relaxed);
    }
};

/****************************************************************************
 * clients
 */
void client::handle(int max_iters)
{
    for (int i = 0; (max_iters == 0 || i < max_iters); ++i) {
        auto [msg, ec] = helper::recvmsg(sock_.native_handle());
        if (ec) {
            if (ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) {
                return;
            }
            if (ec.value() == ENOENT) {
                printf("%s-%d: connection closed by peer\n", role().c_str(), sock_.native_handle());
            } else {
                printf("%s-%d: recv error: %s\n", role().c_str(), sock_.native_handle(),
                       ec.message().c_str());
            }
            broker_.close_client(sock_.native_handle());
            return;
        }

        if (msg.command() == command::close) {
            printf("%s-%d: close\n", role().c_str(), sock_.native_handle());
            broker_.close_client(sock_.native_handle());
            return;
        }

        if (!dispatch(msg)) {
            return;
        }
    }
}

bool publisher::dispatch(const message& msg) const
{
    switch (msg.command()) {
    case command::publish:
#ifdef BROKER_VERBOSE
        printf("pub-%d: publish for [%s]\n", sock_.native_handle(), msg.topic().data());
#endif
        if (!broker_.enqueue_delivery(msg)) {
            return false;
        }
        break;

    default:
        printf("pub-%d: unexpected command [%x]\n", sock_.native_handle(), (uint8_t)msg.command());
        broker_.close_client(sock_.native_handle());
        return false;
    }

    return true;
}

bool subscriber::dispatch(const message& msg) const
{
    switch (msg.command()) {
    case command::ack:
#ifdef BROKER_VERBOSE
        printf("sub-%d: ack for msg %lu\n", sock_.native_handle(), msg.id());
#endif
        broker_.receive_ack(this, msg.id());
        break;

    case command::unsubscribe:
        printf("sub-%d: unsubscribe\n", sock_.native_handle());
        broker_.unsubscribe(this, sock_.native_handle());
        return false;

    default:
        printf("sub-%d: unexpected command [%x]\n", sock_.native_handle(), (uint8_t)msg.command());
        broker_.close_client(sock_.native_handle());
        return false;
    }

    return true;
}

void subscriber::push(const std::shared_ptr<message>& msg)
{
    std::lock_guard<decltype(mtx_)> lk(mtx_);
    drainq_.emplace_back(msg);
    broker_.drain(this);
}

void subscriber::drain_messages()
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
        ::sendmsg(sock_.native_handle(), &msg, MSG_NOSIGNAL);
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
