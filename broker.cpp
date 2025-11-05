#include <sysexits.h>

#include <cmath>

#include "broker.hpp"

using namespace std::chrono;
// using namespace std::string_literals;

namespace toymq {

namespace {

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
static constexpr int DEFAULT_KEEPER_INTERVAL = 60;
static constexpr int DEFAULT_SAMPLER_INTERVAL = 1;

}  // namespace

/****************************************************************************
 * broker
 */
broker::broker(const tbd::config& cfg)
    : cfg_(cfg)
    , signalfd_({SIGHUP, SIGINT, SIGQUIT, SIGTERM}, true)
    , srvsock_((uint16_t)cfg_.get<int>("listen_port", DEFAULT_PORT))
    , logger_(cfg_.get<std::string>("log_procname", "broker"),
              cfg_.get<std::string>("log_filepath", "broker.log"))
    , persist_(cfg_.get<std::string>("persistent_file").empty()
                   ? nullptr
                   : std::make_unique<persistence>(cfg_, logger_,
                                                   cfg_.get<std::string>("persistent_file")))
    , running_(true)
    , thrpool_(cfg_.get<int>("worker_threads", DEFAULT_WORKERS))
    , nreactors_(cfg_.get<int>("reactor_threads", DEFAULT_REACTORS))
    , reactors_(nreactors_)
    , nrouters_(cfg.get<int>("router_threads", DEFAULT_ROUTERS))
    , routers_(nrouters_)
    , sub_last_updated_(std::chrono::steady_clock::now())
{
    logger_.set_level(cfg_.get<std::string>("log_level", "info"));
    logger_.info("----------------------------------------------------------------");
    logger_.info("toy-mq broker start%s", persist_ ? "" : " (w/o persistence)");
    if (likely(persist_)) {
        persist_->load(unacked_msgs_);
        metrics_.unacked = unacked_msgs_.size();
    }
    start_residents();
    epollfd_.add(*signalfd_);
    epollfd_.add(*srvsock_);
}

broker::~broker()
{
    stop_residents();
    thrpool_.wait_all();
    persist_.reset();  // dtor -> wait bg_writer completed
    logger_.info("toy-mq broker terminated");
}

void broker::run()
{
    while (run_one())
        ;
}

bool broker::run_one()
{
    static const auto pub_max = cfg_.get<int>("publish_maxiters", DEFAULT_PUBLISH_MAXITERS);

    try {
        auto events = epollfd_.wait();
        for (const auto& ev : events) {
            if (unlikely(ev.fd == *signalfd_)) {
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
            if (unlikely(it == publishers_.end())) {
                logger_.warn("broker: publisher %d already closed", ev.fd);
                std::error_code ec;
                epollfd_.del(ev.fd, ec);
                continue;
            }

            lkp.unlock();
            it->second->handle(ev.events, pub_max);
        }

        return true;

    } catch (const std::exception& e) {
        logger_.error("broker: unexpected error occurred: %s", e.what());
        return false;
    }
}

bool broker::handle_signal()
{
    static const std::unordered_map<int, const char*> sigabbrev{
        {1, "HUP"}, {2, "INT"}, {3, "QUIT"}, {15, "TERM"}};
    auto sig = signalfd_.get_last_signal();
    logger_.info("broker: receive SIG%s", sigabbrev.at(sig));
    logger_.flush();
    switch (sig) {
    case SIGHUP: {
        // XXX
        tbd::config ncfg("sample.config");
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

void broker::accept_new_client(uint32_t events)
{
    if (unlikely(events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP))) {
        logger_.error("accept_new_client: server socket error, event bits = %08x", events);
        exit(EX_OSERR);
    }

    auto sock = srvsock_.accept();
    auto sockfd = *sock;
again:
    auto [msg, ec] = helper::recvmsg(sockfd, true);
    if (unlikely(ec)) {
        logger_.error("accept_new_client: recv error (fd=%d): %s", sockfd, ec.message().c_str());
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
        std::unique_lock<decltype(mtx_pubs_)> lkp(mtx_pubs_);
        auto [it, _] =
            publishers_.emplace(sockfd, std::make_unique<publisher>(*this, logger_, sock));
        lkp.unlock();
        epollfd_.add(sockfd);
        logger_.info("broker: accept new publisher (fd=%d)", sockfd);
        it->second->receive_publish(msg);
        break;
    }

    case command::subscribe: {
        auto shard = sockfd % nreactors_;
        auto sub = std::make_shared<subscriber>(*this, logger_, sock, msg, shard);
        const auto& subid = sub->client_id();
        std::unique_lock<decltype(mtx_subs_)> lks(mtx_subs_);
        auto it = subscribers_.find(subid);
        if (unlikely(it != subscribers_.end())) {
            if (it->second->leaving()) {
                subscribers_.erase(it);  // last shared_ptr -> dtor
            } else {
                logger_.error("broker: subscriber id %s is already active, reject it", subid);
                (void)helper::sendmsg(*sock, command::nack);
                break;
            }
        }
        subscribers_.emplace(subid, sub);
        sub_last_updated_ = steady_clock::now();
        logger_.info("broker: accept new subscriber %s [%s] (fd=%d, reactor-%d)", subid.c_str(),
                     sub->topic().c_str(), *sub->socket(), sub->shard());
        if (likely(persist_)) {
            persist_->insert_subscriber(sub);
        }
        lks.unlock();
        reactors_[shard]->add_subscriber(sub);
        std::weak_ptr<subscriber> sub_wp(sub);
        thrpool_.submit([this, sub_wp = std::move(sub_wp)] { redeliver(sub_wp); });
        break;
    }

    default:
        break;
    }
    logger_.flush();
}

// worker
void broker::redeliver(const std::weak_ptr<subscriber>& sub_wp)
{
    if (auto sub = sub_wp.lock()) {
        std::lock_guard<decltype(mtx_unack_)> lku(mtx_unack_);
        for (const auto& [_, unacked] : unacked_msgs_) {  // O(n)
            if (unacked.subscribers.find(sub->client_id()) != unacked.subscribers.end()) {
                // O(1)
                sub->push(unacked.msg);
            }
        }
    }
}

// reactor, worker, keeper
void broker::try_attach()
{
    static const auto max = cfg_.get<int>("max_messages", DEFAULT_MAX_MESSAGES);
    static const auto lo = (unsigned long)cfg_.get<int>("low_watermark", DEFAULT_LO_WATERMARK);
    static const auto threshold = (unsigned long)std::ceil(1.0 * max * lo / 100);

    if (metrics_.pending + metrics_.unacked <= threshold) {
        std::shared_lock<decltype(mtx_pubs_)> lkp(mtx_pubs_);
        for (auto&& [sockfd, pub] : publishers_) {  // O(n)
            if (pub->attach_if_detached()) {
                logger_.info("broker: pub-%d attached back!", sockfd);
                epollfd_.add(sockfd);
            }
        }
    }
}

/****************************************************************************
 * residents
 */
void broker::start_residents()
{
    static const auto sampler_interval =
        cfg_.get<int>("sampler_interval_sec", DEFAULT_SAMPLER_INTERVAL) * 1000;
    static const auto keeper_interval =
        cfg_.get<int>("keeper_interval_sec", DEFAULT_KEEPER_INTERVAL) * 1000;

    // (thrpool ->) sampler -> keeper -> reactor -> router
    sampler_.start(sampler_interval, [this] { sampling(); });
    keeper_.start(keeper_interval, [this] { clean_expired_msg(); });

    for (size_t i = 0; i < nreactors_; ++i) {
        reactors_[i] = std::make_unique<reactor>(cfg_, logger_, i);
    }
    for (size_t i = 0; i < nrouters_; ++i) {
        routers_[i].i = i;
        routers_[i].thr = std::thread([this, i] { msg_router(i); });
    }
}

void broker::stop_residents()
{
    // router -> reactor -> keeper -> sampler (-> thrpool)
    running_ = false;
    std::for_each(routers_.begin(), routers_.end(), [](auto&& r) { r.cv.notify_one(); });
    std::for_each(reactors_.begin(), reactors_.end(), [](auto&& r) { r->stop(); });
    keeper_.stop();
    sampler_.stop();
    std::for_each(routers_.begin(), routers_.end(), [](auto&& r) { join_thread(r.thr); });
}

void broker::msg_router(size_t i)
{
    static const auto ttl = seconds(cfg_.get<int>("message_ttl_sec", DEFAULT_MESSAGE_TTL));
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
        if (rt.snap.empty() || rt.last_snapped < sub_last_updated_.load()) {
            rt.snap.clear();
            std::shared_lock<decltype(mtx_subs_)> lks(mtx_subs_);
            rt.snap.reserve(subscribers_.size());
            for (const auto& [_, sub] : subscribers_) {  // O(n)
                rt.snap.emplace_back(sub);
            }
            rt.last_snapped = steady_clock::now();
        }

        // topic matching
        unacked_msg unacked;
        for (const auto& sub : rt.snap) {
            if (sub->match(msg->topic())) {
                unacked.subscribers.emplace(sub->client_id(), sub);
            }
        }

        // enqueue to emit
        unacked.msg = std::move(msg);
        unacked.expiry = system_clock::now() + ttl;
        push_to_subscribers(rt, unacked);

        lkr.lock();
    }
}

void broker::push_to_subscribers(router& rt, unacked_msg& unacked)
{
    static const size_t max = cfg_.get<int>("max_messages", DEFAULT_MAX_MESSAGES);
    static const auto& policy = cfg_.get<std::string>("overflow_policy");

    if (!unacked.subscribers.empty()) {
        metrics_.unacked.fetch_add(1, std::memory_order_relaxed);
        const auto msgid = unacked.msg->id();
        logger_.debug("router-%lu: process msg %lu (backlog %lu)", rt.i, msgid, backlog());
        std::lock_guard<decltype(mtx_unack_)> lku(mtx_unack_);
        if (policy == "drop" && backlog() >= max) {
            auto it = unacked_msgs_.begin();  // O(1)
            logger_.warn("router-%lu: backlog overflow, drop oldest msg %lu", rt.i, it->first);
            unacked_msgs_.erase(it);  // O(1)
            metrics_.unacked.fetch_sub(1, std::memory_order_relaxed);
            metrics_.dropped.fetch_add(1, std::memory_order_relaxed);
        }
        assert(unacked_msgs_.find(msgid) == unacked_msgs_.end());  // O(log n)
        for (auto&& [_, sub_wp] : unacked.subscribers) {
            if (auto sub = sub_wp.lock()) {
                sub->push(unacked.msg);
            } else {
                rt.snap.clear();
                logger_.warn("router-%lu: subscriber already closed before push", rt.i);
            }
        }

        if (likely(persist_)) {
            persist_->insert_message(unacked);
        }
        unacked_msgs_.emplace(msgid, std::move(unacked));  // O(log n)
    }
}

void broker::clean_expired_msg()
{
    std::unique_lock<decltype(mtx_unack_)> lku(mtx_unack_);
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
    lku.unlock();
    try_attach();
}

#define FMT_RATE "rate [ingress %lu, egress %lu, ack %lu]"
#define FMT_GAUGE "gauge [pending %lu, unacked %lu]"
#define FMT_COUNTER "counter [ingress %lu, ack %lu, rejected %lu, dropped %lu]"
void broker::sampling()
{
    static uint64_t total_ingress = 0;
    static uint64_t total_ack = 0;

    auto ingress = metrics_.ingress.exchange(0, std::memory_order_relaxed);
    auto egress = metrics_.egress.exchange(0, std::memory_order_relaxed);
    auto ack = metrics_.ack.exchange(0, std::memory_order_relaxed);
    total_ingress += ingress;
    total_ack += ack;

    logger_.info("sampler: " FMT_RATE ",\t" FMT_GAUGE ",\t" FMT_COUNTER,
                 // rate
                 ingress, egress, ack,
                 // gauge
                 metrics_.pending.load(std::memory_order_relaxed),
                 metrics_.unacked.load(std::memory_order_relaxed),
                 // counter
                 total_ingress, total_ack, metrics_.rejected.load(std::memory_order_relaxed),
                 metrics_.dropped.load(std::memory_order_relaxed));
    logger_.flush();
}

/****************************************************************************
 * from publisher
 */
// main -> router
std::pair<bool, uint64_t> broker::enqueue_deliver(const message& orig_msg, int sockfd)
{
    static const auto& policy = cfg_.get<std::string>("overflow_policy");

    if (policy == "reject" && !under_watermark(orig_msg)) {
        return {false, 0};
    }

    metrics_.ingress.fetch_add(1, std::memory_order_relaxed);
    metrics_.pending.fetch_add(1, std::memory_order_relaxed);

    auto msgid = next_id();
    auto new_msg = std::make_shared<message>(orig_msg, msgid);

    auto& rt = routers_[sockfd % nrouters_];
    logger_.debug("broker: enqueue msg %lu [%s] to router-%lu", msgid, orig_msg.topic(), rt.i);
    std::lock_guard<decltype(rt.mtx)> lkr(rt.mtx);
    rt.queue.emplace_back(std::move(new_msg));
    rt.cv.notify_one();

    return {true, msgid};
}

// main
uint64_t broker::next_id() noexcept
{
    static const bool debug_seqid = cfg_.get<bool>("debug_seqid", false);

    if (unlikely(debug_seqid)) {
        static std::atomic<uint64_t> msgid_ = 0;
        return ++msgid_;
    }

    static uint64_t previd_ = 0;
    uint64_t attempts = 0;
again:
    ++attempts;
    uint64_t newid = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
    if (unlikely(newid <= previd_)) {
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
bool broker::under_watermark(const message& msg)
{
    static const auto max = cfg_.get<int>("max_messages", DEFAULT_MAX_MESSAGES);
    static const auto hi = cfg_.get<int>("high_watermark", DEFAULT_HI_WATERMARK);
    static const auto threshold = (unsigned long)std::ceil(1.0 * max * hi / 100);

    bool under = backlog() < threshold;
    if (likely(under)) {
        logger_.trace("broker: backlog %lu, threshold %lu", backlog(), threshold);
    } else {
        logger_.warn("broker: backlog over watermark, reject msg for [%s] (backlog %lu)",
                     msg.topic().data(), backlog());
        metrics_.rejected.fetch_add(1, std::memory_order_relaxed);
    }
    return under;
}

// main
void broker::detach_publisher(int sockfd)
{
    epollfd_.del(sockfd);
}

// main
void broker::close_publisher(tbd::io_socket& sock)
{
    int sockfd = *sock;
    std::lock_guard<decltype(mtx_pubs_)> lkp(mtx_pubs_);
    auto it = publishers_.find(sockfd);
    if (it != publishers_.end()) {
        epollfd_.del(sockfd);
        sock.close();
        publishers_.erase(it);
        logger_.info("broker: close publisher (fd=%d)", sockfd);
        logger_.flush();
    }
}

/****************************************************************************
 * from subscriber
 */
// reactor
void broker::receive_ack(std::weak_ptr<subscriber>&& sub_wp, const message& msg)
{
    metrics_.ack.fetch_add(1, std::memory_order_relaxed);
    auto msgid = msg.id();

    std::unique_lock<decltype(mtx_unack_)> lku(mtx_unack_, std::defer_lock);
    if (lku.try_lock()) {  // inline path
        collect_unack(std::move(sub_wp), msgid, lku, true);
        return;
    }

    // later ...
    thrpool_.submit([this, sub_wp = std::move(sub_wp), msgid]() mutable {
        std::unique_lock<decltype(mtx_unack_)> lku(mtx_unack_);
        collect_unack(std::move(sub_wp), msgid, lku, false);
    });
}

// reactor or worker (under locked for unacked_msgs_)
void broker::collect_unack(std::weak_ptr<subscriber>&& sub_wp, uint64_t msgid,
                           std::unique_lock<decltype(mtx_unack_)>& lku, bool fastpath)
{
    std::string subid;
    bool dec_unacked = false;
    if (auto sub = sub_wp.lock()) {
        subid = sub->client_id();
        sub->ack_arrived(msgid);
        dec_unacked = remove_unacked(msgid, subid);
    }
    lku.unlock();
    if (dec_unacked) {
        const auto& role = fastpath ? "reactor" : "worker";
        logger_.debug("%s: all subscribers sent ack for msg %lu", role, msgid);
        try_attach();
    }

    if (likely(persist_ && !subid.empty())) {
        std::vector<std::pair<uint64_t, bool>> removed_msgids({{msgid, dec_unacked}});
        persist_->delete_delivers(subid, removed_msgids);
    }
}

// reactor -> worker
void broker::unsubscribe(const std::shared_ptr<subscriber>& sub)
{
    std::unique_lock<decltype(mtx_subs_)> lks(mtx_subs_);
    subscribers_.erase(sub->client_id());
    lks.unlock();
    sub_last_updated_ = steady_clock::now();

    thrpool_.submit([this, subid = sub->client_id(), unacked_msgids = sub->unacked_msgids()] {
        bool dec_unacked = false;
        std::unique_lock<decltype(mtx_unack_)> lku(mtx_unack_);
        std::vector<std::pair<uint64_t, bool>> removed_msgids;
        removed_msgids.reserve(unacked_msgids.size());
        for (const auto& msgid : unacked_msgids) {  // O(n)
            bool done = remove_unacked(msgid, subid);
            removed_msgids.emplace_back(std::make_pair(msgid, done));
            dec_unacked |= done;
        }
        lku.unlock();

        if (likely(persist_)) {
            persist_->delete_subscriber(subid);
            persist_->delete_delivers(subid, removed_msgids);
        }

        if (dec_unacked) {
            try_attach();
        }
    });
}

// reactor, worker
bool broker::remove_unacked(uint64_t msgid, const std::string& subid)
{
    auto it = unacked_msgs_.find(msgid);  // O(log n)
    if (likely(it != unacked_msgs_.end())) {
        it->second.subscribers.erase(subid);  // O(1)
        if (it->second.subscribers.empty()) {
            unacked_msgs_.erase(it);  // O(1)
            metrics_.unacked.fetch_sub(1, std::memory_order_relaxed);
            return true;
        }
    }
    return false;
}

// router
void broker::enqueue_flush(std::weak_ptr<subscriber>&& sub_wp)
{
    try {
        thrpool_.submit([sub_wp = std::move(sub_wp)] {
            if (auto sub = sub_wp.lock()) {
                sub->flush();
            }
        });
    } catch (const std::exception& e) {
        logger_.error("broker: unexpected error occurred: %s", e.what());
    }
}

void broker::on_emit(uint64_t count)
{
    metrics_.egress.fetch_add(count, std::memory_order_relaxed);
}

// reactor
void broker::close_subscriber(const std::shared_ptr<subscriber>& sub, bool unsub)
{
    const auto& subid = sub->client_id();
    reactors_[sub->shard()]->notify_close(sub->client_id());
    logger_.info("broker: %s subscriber %s", unsub ? "unsubscribe" : "close", subid.c_str());

    if (unsub) {
        unsubscribe(sub);
    }
}

}  // namespace toymq

int main()
{
    tbd::config cfg("sample.config");
    toymq::broker brk(cfg);
    brk.run();
}
