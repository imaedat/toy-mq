#include <chrono>
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

class broker;

/****************************************************************************
 * clients
 */
class client
{
    static constexpr char MAGIC[4] = {'M', 'Q', 'C', 'L'};
    static constexpr size_t VIRTUAL_OFFSET_MAGIC = 8;  // offsetof(client, magic_)

  public:
    static bool is_alive(const client* cli)
    {
        return ::memcmp(((uint8_t*)cli) + VIRTUAL_OFFSET_MAGIC, MAGIC, sizeof(MAGIC)) == 0;
    }

    virtual ~client() noexcept
    {
        ::memset(magic_, 0, sizeof(magic_));
    }
    void handle() const;

  protected:
    char magic_[4] = {0};
    broker& broker_;
    io_socket sock_;
    std::string role_;

    client(broker& b, io_socket& s) noexcept
        : broker_(b)
        , sock_(std::move(s))
    {
        ::memcpy(magic_, MAGIC, sizeof(magic_));
    }

    const std::string& role() const noexcept
    {
        return role_;
    }

    virtual bool dispatch(const message& msg) const = 0;
};

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
            auto n = topic_.size() - 1;
            if (n >= 3 && topic_[n - 1] == '.' && topic_[n] == '*') {
                topic_.erase(n);  // "foo.bar.*" => "foo.bar."
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

        printf("sub-%d: my topic [%s], msg topic [%s] => match=%s\n", sock_.native_handle(),
               topic_.c_str(), msg_topic.data(), match ? "true" : "false");
        return match;
    }

    void push(const message& msg) const
    {
        // std::lock_guard<decltype(mtx_)> lk(mtx_);
        sock_.send(msg.hdr(), msg.length());
    }

  private:
    std::string topic_;
    // mutable std::mutex mtx_;

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
    static constexpr unsigned DEFAULT_MESSAGE_TTL = 3600;

  public:
    explicit broker(const config& cfg)
        : cfg_(cfg)
        , srv_((uint16_t)cfg_.get<int>("listen_port", DEFAULT_PORT))
        // , tpool_(cfg_.get<int>("thread_pool", DEFAULT_WORKERS))
        , running_(true)
    {
        keeper_ = std::thread([this] { house_keeper(); });
        epollfd_.add(srv_.native_handle());
    }

    ~broker()
    {
        {
            std::lock_guard<decltype(mtx_)> lk(mtx_);
            running_ = false;
            cv_.notify_one();
        }
        if (keeper_.joinable()) {
            keeper_.join();
        }
    }

    void run()
    {
        while (true) {
            run_one();
        }
    }

  private:
    config cfg_;
    tcp_server srv_;
    // thread_pool tpool_;
    std::mutex mtx_;
    epollfd epollfd_;
    std::thread keeper_;
    std::condition_variable cv_;
    bool running_;

    // { fd => client }
    std::unordered_map<int, std::unique_ptr<client>> clients_;
    std::unordered_set<const subscriber*> subscribers_;

    // { msgid => { msg, subscribers } }
    struct unacked
    {
        message msg;
        std::chrono::system_clock::time_point expiry;
        std::unordered_set<const subscriber*> subs;
    };
    std::map<uint64_t, unacked> unacked_msgs_;

    void run_one()
    {
        auto events = epollfd_.wait();
        for (const auto& ev : events) {
            if (ev.fd == srv_.native_handle()) {
                accept_new_client();
            } else {
                clients_[ev.fd]->handle();
            }
        }
    }

    void accept_new_client()
    {
        auto sock = srv_.accept();
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
            deliver(msg);
            // `sock` closed on scope end, return w/o epoll registration
            return;
        }

        case command::publish: {
            printf("broker: accept new publisher (fd=%d)\n", fd);
            epollfd_.add(fd);
            clients_.emplace(fd, std::make_unique<publisher>(*this, sock));
            deliver(msg);
            break;
        }

        case command::subscribe: {
            printf("broker: accept new subscriber (fd=%d)\n", fd);
            epollfd_.add(fd);
            clients_.emplace(fd, std::make_unique<subscriber>(*this, sock, msg.topic()));
            auto* sub = (subscriber*)clients_[fd].get();
            subscribers_.emplace(sub);
            redeliver(sub);
            break;
        }

        default:
            break;
        }
    }

    uint64_t next_id() const noexcept
    {
#if 1
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
#else
        static uint64_t msgid_ = 0;
        return ++msgid_;
#endif
    }

    void redeliver(const subscriber* sub)
    {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        for (const auto& p : unacked_msgs_) {  // O(n)
            const auto& msg = p.second.msg;
            if (sub->match(msg.topic())) {
                sub->push(msg);
            }
        }
    }

    void house_keeper()
    {
        std::unique_lock<decltype(mtx_)> lk(mtx_);
        while (true) {
            cv_.wait_for(lk, minutes(1), [this] { return !running_; });
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
            }
        }
    }

    /******************************************************************
     * from client
     */
    friend class client;
    friend class publisher;
    friend class subscriber;
    void close_client(int fd)
    {
        epollfd_.del(fd);
        auto it = clients_.find(fd);  // O(1)
        if (it != clients_.end()) {
            auto* cli = it->second.get();
            subscribers_.erase((subscriber*)cli);  // O(1)
            clients_.erase(it);
        }
    }

    /******************************************************************
     * from publisher
     */
    void deliver(const message& orig_msg)
    {
        printf("broker::deliver: msg data [%s]\n", (char*)orig_msg.data());
        auto msgid = next_id();

        message new_msg(orig_msg);  // copy
        new_msg.resize(new_msg.length() + sizeof(msgid));
        new_msg.id(msgid);

        // tpool_.submit([this, msgid, msg = std::move(new_msg), subs = subscribers_]() mutable {
        const auto& topic = new_msg.topic();
        unacked unacked;
        // for (const auto& sub : subs) {
        for (const auto& sub : subscribers_) {
            if (sub->match(topic)) {
                unacked.subs.emplace(sub);
            }
        }
        if (unacked.subs.empty()) {
            return;
        }

        unacked.msg = std::move(new_msg);
        unacked.expiry =
            system_clock::now() + seconds(cfg_.get<int>("message_ttl_sec", DEFAULT_MESSAGE_TTL));
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        printf("broker: current unacked msgs = %lu, enqueue %lu\n", unacked_msgs_.size(), msgid);
        // TODO pub too fast, ack waiting. back pressure?
        if (unacked_msgs_.size() >= (size_t)cfg_.get<int>("max_messages", DEFAULT_MAX_MESSAGES)) {
            unacked_msgs_.erase(unacked_msgs_.begin());  // O(log n)
        }
        assert(unacked_msgs_.find(msgid) == unacked_msgs_.end());
        unacked_msgs_.emplace(msgid, std::move(unacked));  // O(log n)
        auto it = unacked_msgs_.rbegin();                  // O(1)
        const auto& msg = it->second.msg;
        for (const auto& sub : it->second.subs) {
            // if (!client::is_alive((client*)sub)) {
            //     printf("broker: !!! subscriber %p is already broken !!!!\n", sub);
            //     continue;
            // }
            sub->push(msg);
        }
        // });
    }

    /******************************************************************
     * from subscriber
     */
    void receive_ack(const subscriber* sub, uint64_t msgid)
    {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        auto it = unacked_msgs_.find(msgid);  // O(log n)
        assert(it != unacked_msgs_.end());
        it->second.subs.erase(sub);  // O(1)
        if (it->second.subs.empty()) {
            unacked_msgs_.erase(it);  // O(1)
            printf("broker: all subs sent ack for msg %lu\n", msgid);
        }
    }

    void unsubscribe(const subscriber* sub, int fd)
    {
        close_client(fd);

        std::lock_guard<decltype(mtx_)> lk(mtx_);
        auto it = unacked_msgs_.begin();
        while (it != unacked_msgs_.end()) {  // O(n)
            auto& subs = it->second.subs;
            auto jt = subs.find(sub);  // O(1)
            if (jt != subs.end()) {
                subs.erase(jt);
                if (subs.empty()) {
                    it = unacked_msgs_.erase(it);  // O(1)
                    continue;
                }
            }
            ++it;
        }
    }
};

/****************************************************************************
 * clients
 */
void client::handle() const
{
    do {
        auto [msg, ec] = helper::recvmsg(sock_.native_handle());
        if (ec) {
            if (ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) {
                return;
            }
            printf("%s-%d: rev error: %s\n", role().c_str(), sock_.native_handle(),
                   ec.message().c_str());
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
    } while (false);
}

bool publisher::dispatch(const message& msg) const
{
    switch (msg.command()) {
    case command::publish:
        printf("pub-%d: publish for [%s]\n", sock_.native_handle(), msg.topic().data());
        broker_.deliver(msg);
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
        printf("sub-%d: ack for %lu\n", sock_.native_handle(), msg.id());
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

}  // namespace toymq

int main()
{
    config cfg("sample.config");
    toymq::broker brk(cfg);
    brk.run();
}
