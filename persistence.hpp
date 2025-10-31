#ifndef TOYMQ_PERSISTENCE_HPP_
#define TOYMQ_PERSISTENCE_HPP_

#include <chrono>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "logger.hpp"
#include "sqlite.hpp"

#include "broker.hpp"
#include "proto.hpp"

namespace toymq {

class persistence
{
  public:
    persistence(const tbd::config& cfg, tbd::logger& l, std::string_view dbfile)
        : logger_(l)
        , db_(dbfile)
        , bg_writer_(1)
    {
        db_.exec("create table if not exists messages ("
                 "  id integer primary key,"
                 "  expiry integer not null,"
                 "  size integer not null,"
                 "  topic text not null,"
                 "  body_size integer not null,"
                 "  body blob"
                 ");");

        db_.exec("create table if not exists subscribers ("
                 "  id text not null primary key,"
                 "  topic text not null"
                 ") without rowid;");

        db_.exec("create table if not exists delivers ("
                 "  message_id integer primary key,"
                 "  subscriber_id text not null,"
                 "  foreign key (message_id) references messages(id),"
                 "  foreign key (subscriber_id) references subscribers(id)"
                 ");");

        db_.exec(
            "create index if not exists idx_deliv_subid on delivers(subscriber_id, message_id);");

        const auto& jmode = cfg.get<std::string>("persistent_journal", "wal");
        db_.exec(std::string("pragma journal_mode = ") + jmode + ";");

        const auto& sync = cfg.get<std::string>("persistent_sync", "normal");
        db_.exec(std::string("pragma synchronous = ") + sync + ";");
    }

    ~persistence()
    {
        auto qsz = bg_writer_.queue_size();
        if (qsz >= 100'000) {
            logger_.info("broker: bg_writer holds too many flush tasks (%lu), wait ...", qsz);
            logger_.flush();
        }
        bg_writer_.wait_all();
        db_.exec("vacuum;");
    }

    void insert_subscriber(const std::shared_ptr<subscriber>& sub)
    {
        auto n = db_.exec(
            "insert into subscribers (id, topic) values (?, ?) on conflict(id) do update set topic = ?;",
            {sub->client_id(), sub->topic(), sub->topic()});
        if (n > 0) {
            logger_.debug("broker: insert into subscribers subid %s", sub->client_id());
        }
    }

    void delete_subscriber(const std::string& subid)
    {
        auto n = db_.exec("delete from subscribers where id = ?;", {subid});
        if (n > 0) {
            logger_.debug("broker: delete from subscribers subid %s", subid.c_str());
        }
    }

    void insert_message(broker::unacked_msg& unacked)
    {
        using namespace std::chrono;

        std::weak_ptr<message> msg_wp(unacked.msg);
        bg_writer_.submit([this, msg_wp = std::move(msg_wp), expiry = unacked.expiry,
                           subscribers = unacked.subscribers] {
            if (auto msg = msg_wp.lock()) {
                auto it = subscribers.cbegin();
                std::ostringstream ss;
                ss << "insert into delivers (message_id, subscriber_id) values (" << msg->id()
                   << ", '" << it->first;
                for (++it; it != subscribers.cend(); ++it) {
                    ss << "'), (" << msg->id() << ", '" << it->first;
                }
                ss << "');";

                db_.begin([&](auto& txn) {
                    auto n = txn.exec(
                        "insert into messages (id, expiry, size, topic, body_size, body) values (?, ?, ?, ?, ?, ?);",
                        {(int64_t)msg->id(),
                         duration_cast<nanoseconds>(expiry.time_since_epoch()).count(),
                         (int64_t)msg->length(), std::string(msg->topic()),
                         (int64_t)msg->data_size(), std::make_pair(msg->data(), msg->data_size())});
                    if (n > 0) {
                        logger_.debug("writer: insert into messages msgid %lu", msg->id());
                    }

                    auto m = txn.exec(ss.str());
                    if (m > 0) {
                        logger_.debug("writer: insert into delivers for %ld subscribers", m);
                    }
                });

            } else {
                // logger_.debug("writer: msg already expired (i.e. acked)");
            }
        });
    }

    void delete_delivers(const std::string& subid,
                         std::vector<std::pair<uint64_t, bool>>& removed_msgids)
    {
        if (removed_msgids.empty()) {
            return;
        }

        bg_writer_.submit([this, subid, removed_msgids = std::move(removed_msgids)] {
            uint64_t msgid1st = (removed_msgids.size() == 1) ? removed_msgids[0].first : 0;
            std::ostringstream ss;
            ss << "delete from delivers where subscriber_id = '" << subid
               << "' and message_id in (-1";
            for (const auto& [msgid, done] : removed_msgids) {
                ss << ", " << msgid;
            }
            ss << ");";
            auto n = db_.exec(ss.str());
            if (n > 0) {
                if (msgid1st > 0) {
                    logger_.debug("writer: delete from delivers subid %s, msgid %lu", subid.c_str(),
                                  msgid1st);
                } else {
                    logger_.debug("writer: delete from delivers subid %s, %ld msgs", subid.c_str(),
                                  n);
                }
            }

            ss.str("");
            ss << "delete from messages where id in (-1";
            for (const auto& [msgid, done] : removed_msgids) {
                if (done) {
                    ss << ", " << msgid;
                }
            }
            ss << ");";
            auto m = db_.exec(ss.str());
            if (m > 0) {
                if (msgid1st > 0) {
                    logger_.debug("writer: delete from messages msgid %ld", msgid1st);
                } else {
                    logger_.debug("writer: delete from messages %ld msgs", m);
                }
            }
        });
    }

    void load(std::map<uint64_t, broker::unacked_msg>& unacked_msgs)
    {
        using namespace std::chrono;

        auto cur1 =
            db_.cursor_for("select id, expiry, size, topic, body_size, body from messages;");
        while (true) {
            auto row = cur1.next();
            if (!row) {
                break;
            }
            broker::unacked_msg unacked;
            unacked.msg = std::make_shared<message>((*row)[2].to_i());
            unacked.msg->topic((*row)[3].to_s());
            const auto& b = (*row)[5].to_b();
            unacked.msg->data(b.data(), b.size());
            unacked.msg->id((*row)[0].to_i());
            unacked.expiry = system_clock::time_point(nanoseconds((*row)[1].to_i()));

            unacked_msgs.emplace((*row)[0].to_i(), std::move(unacked));
        }

        uint64_t msgid_prev = 0;
        auto it = unacked_msgs.begin();
        auto cur2 =
            db_.cursor_for("select message_id, subscriber_id from delivers order by message_id;");
        while (true) {
            auto row = cur2.next();
            if (!row) {
                break;
            }
            auto msgid = (uint64_t)(*row)[0].to_i();
            if (msgid_prev != msgid) {
                it = unacked_msgs.find(msgid);
                if (it == unacked_msgs.end()) {
                    continue;
                }
            }
            msgid_prev = msgid;
            it->second.subscribers.emplace((*row)[1].to_s(), std::shared_ptr<subscriber>(nullptr));
        }

        if (!unacked_msgs.empty()) {
            logger_.info("broker: load %lu unacked msgs", unacked_msgs.size());
        }
    }

  private:
    tbd::logger& logger_;
    tbd::sqlite db_;
    tbd::thread_pool bg_writer_;
};

}  // namespace toymq

#endif
