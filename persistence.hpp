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

namespace {
std::string bin2hex(const void* buf, size_t size)
{
    static constexpr const char hex[] = "0123456789abcdef";
    const auto* p = (const uint8_t*)buf;
    std::string s;
    s.reserve(size * 2 + 1);
    s.resize(size * 2);
    for (size_t i = 0; i < size; ++i) {
        s[i * 2] = hex[(p[i] >> 4) & 0x0FU];
        s[i * 2 + 1] = hex[p[i] & 0x0FU];
    }
    return s;
}
}  // namespace

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

        // wal / off / persist
        const auto& jmode = cfg.get<std::string>("persistent_journal", "wal");
        db_.exec(std::string("pragma journal_mode = ") + jmode + ";");

        // off
        const auto& sync = cfg.get<std::string>("persistent_sync", "normal");
        db_.exec(std::string("pragma synchronous = ") + sync + ";");
    }

    ~persistence() = default;

    void insert_subscriber(const std::string& subid, std::string_view topic)
    {
        std::string insert_stmt;
        insert_stmt.reserve(128);
        insert_stmt.append("insert into subscribers (subscriber_id, topic) values ('")
            .append(subid)
            .append("', '")
            .append(topic)
            .append("') on conflict(subscriber_id) do update set topic = '")
            .append(topic)
            .append("';");
        auto n = db_.exec(insert_stmt);
        if (n > 0) {
            logger_.debug("broker: insert into subscribers subid %s", subid.c_str());
        }
    }

    void delete_subscriber(const std::string& subid)
    {
        if (!subid.empty()) {
            std::string delete_stmt;
            delete_stmt.reserve(128);
            delete_stmt.append("delete from subscribers where subscriber_id = '")
                .append(subid)
                .append("';");
            auto n = db_.exec(delete_stmt);
            if (n > 0) {
                logger_.debug("broker: delete from subscribers subid %s", subid.c_str());
            }
        }
    }

    /* ```
     * sqlite3_stmt* stmt;
     * sqlite3_prepare_v2(db,
     *     "INSERT INTO message (message_id, expiry, topic, body) VALUES (?, ?, ?, ?)",
     *     -1, &stmt, nullptr);
     * sqlite3_bind_int64(stmt, 1, msg->id());
     * sqlite3_bind_int64(stmt, 2, duration_cast<namespace>(expiry.time_since_epoch()).count());
     * sqlite3_bind_text(stmt, 3, msg->topic().data(), msg->topic().size());
     * sqlite3_bind_blob(stmt, 4, msg->data(), msg->data_size(), SQLITE_TRANSIENT);
     * sqlite3_step(stmt);
     * sqlite3_finalize(stmt);
     * ```
     */
    void insert_message(std::weak_ptr<message>& msg_wp,
                        std::chrono::system_clock::time_point expiry,
                        std::vector<std::string>& pushed_subs)
    {
        using namespace std::chrono;

        bg_writer_.submit([this, msg_wp = std::move(msg_wp), expiry,
                           pushed_subs = std::move(pushed_subs)] {
            if (auto msg = msg_wp.lock()) {
                std::ostringstream ss;
                ss << "insert into messages (message_id, expiry, topic, body) values ('"
                   << msg->id() << "', '"
                   << duration_cast<nanoseconds>(expiry.time_since_epoch()).count() << "', '"
                   << msg->topic() << "', X'" << bin2hex(msg->data(), msg->data_size()) << "');";
                auto n = db_.exec(ss.str());
                if (n > 0) {
                    logger_.debug("broker: insert into messages msgid %lu", msg->id());
                }

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

  private:
    tbd::logger& logger_;
    tbd::sqlite db_;
    tbd::thread_pool bg_writer_;
};

}  // namespace toymq

#endif
