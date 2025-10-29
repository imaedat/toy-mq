#ifndef TOYMQ_PERSISTENCE_HPP_
#define TOYMQ_PERSISTENCE_HPP_

#include <chrono>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "logger.hpp"
#include "sqlite.hpp"

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
                 "  message_id integer not null primary key,"
                 "  expiry integer not null,"
                 "  topic text not null,"
                 "  body blob"
                 ");");

        db_.exec("create table if not exists subscribers ("
                 "  subscriber_id text not null primary key,"
                 "  topic text not null"
                 ");");

        db_.exec("create table if not exists delivers ("
                 "  message_id integer not null primary key,"
                 "  subscriber_id text not null,"
                 "  foreign key (message_id) references messages(message_id),"
                 "  foreign key (subscriber_id) references subscribers(subscriber_id)"
                 ");");

        db_.exec(
            "create index if not exists idx_deliv_subid on delivers(subscriber_id, message_id);");

        const auto& jmode = cfg.get<std::string>("persistent_journal", "wal");
        db_.exec(std::string("pragma journal_mode = ") + jmode + ";");

        const auto& sync = cfg.get<std::string>("persistent_sync", "normal");
        db_.exec(std::string("pragma synchronous = ") + sync + ";");
    }

    ~persistence() = default;

    void insert_subscriber(const std::string& subid, const std::string& topic)
    {
        auto n = db_.exec("insert into subscribers (subscriber_id, topic) values (?, ?);",
                          {subid, topic});
        if (n > 0) {
            logger_.debug("broker: insert into subscribers subid %s", subid.c_str());
        }
    }

    void delete_subscriber(const std::string& subid)
    {
        auto n = db_.exec("delete from subscribers where subscriber_id = ?;", {subid});
        if (n > 0) {
            logger_.debug("broker: delete from subscribers subid %s", subid.c_str());
        }
    }

    void insert_message(std::weak_ptr<message>& msg_wp,
                        std::chrono::system_clock::time_point expiry,
                        std::vector<std::string>& pushed_subs)
    {
        using namespace std::chrono;

        bg_writer_.submit([this, msg_wp = std::move(msg_wp), expiry,
                           pushed_subs = std::move(pushed_subs)] {
            if (auto msg = msg_wp.lock()) {
                auto n = db_.exec(
                    "insert into messages (message_id, expiry, topic, body) values (?, ?, ?, ?);",
                    {(int64_t)msg->id(),
                     duration_cast<nanoseconds>(expiry.time_since_epoch()).count(),
                     std::string(msg->topic()),
                     tbd::sqlite::raw_buffer{msg->data(), msg->data_size()}});
                if (n > 0) {
                    logger_.debug("broker: insert into messages msgid %lu", msg->id());
                }

                std::ostringstream ss;
                ss.str("");
                ss << "insert into delivers (message_id, subscriber_id) values ('" << msg->id()
                   << "', '" << pushed_subs[0];
                for (size_t i = 1; i < pushed_subs.size(); ++i) {
                    ss << "'), ('" << msg->id() << "', '" << pushed_subs[i];
                }
                ss << "');";
                auto m = db_.exec(ss.str());
                if (m > 0) {
                    logger_.debug("broker: insert into delivers for %ld subscribers", m);
                }

            } else {
                // logger_.debug("broker: msg already expired (i.e. acked)");
            }
        });
    }

    void delete_delivers(std::string& subid, std::vector<std::pair<uint64_t, bool>>& removed_msgids)
    {
        if (removed_msgids.empty()) {
            return;
        }

        bg_writer_.submit(
            [this, subid = std::move(subid), removed_msgids = std::move(removed_msgids)] {
                uint64_t msgid1st = (removed_msgids.size() == 1) ? removed_msgids[0].first : 0;
                std::ostringstream ss;
                ss << "delete from delivers where subscriber_id = '" << subid
                   << "' and message_id in ('__dummy__";
                for (const auto& [msgid, done] : removed_msgids) {
                    ss << "', '" << msgid;
                }
                ss << "');";
                auto n = db_.exec(ss.str());
                if (n > 0) {
                    if (msgid1st > 0) {
                        logger_.debug("broker: delete from delivers subid %s, msgid %lu",
                                      subid.c_str(), msgid1st);
                    } else {
                        logger_.debug("broker: delete from delivers subid %s, %ld msgs",
                                      subid.c_str(), n);
                    }
                }

                ss.str("");
                ss << "delete from messages where message_id in ('__dummy__";
                for (const auto& [msgid, done] : removed_msgids) {
                    if (done) {
                        ss << "', '" << msgid;
                    }
                }
                ss << "');";
                auto m = db_.exec(ss.str());
                if (m > 0) {
                    if (msgid1st > 0) {
                        logger_.debug("broker: delete from messages msgid %ld", msgid1st);
                    } else {
                        logger_.debug("broker: delete from messages %ld msgs", m);
                    }
                }
            });
    }

    // TODO load()

  private:
    tbd::logger& logger_;
    tbd::sqlite db_;
    tbd::thread_pool bg_writer_;
};

}  // namespace toymq

#endif
