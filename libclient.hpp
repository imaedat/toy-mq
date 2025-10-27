#ifndef TOYMQ_LIB_CLIENT_HPP_
#define TOYMQ_LIB_CLIENT_HPP_

#include "proto.hpp"

#include <uuid/uuid.h>

#include <fstream>

#include "socket.hpp"

namespace toymq {

class publisher
{
  public:
    publisher(std::string_view ipaddr, uint16_t port)
        : sock_(ipaddr, port)
    {
        sock_.set_nonblock(false);
    }

    bool publish(std::string_view topic, std::string_view msg, bool require_ack = false)
    {
        return publish(topic, msg.data(), msg.size(), require_ack);
    }

    bool publish(std::string_view topic, const void* data, size_t size, bool require_ack)
    {
        sock_.connect();
        if (once_flag_) {
            auto [_, lport] = sock_.local_endpoint();
            printf("publisher: localport = %u\n", lport);
            once_flag_ = false;
        }
        auto c = require_ack ? command::publish_ack : command::publish;
        if (auto ec = helper::sendmsg(*sock_, c, topic, data, size)) {
            throw std::system_error(ec, "sendmsg");
        }
        if (require_ack) {
            auto [msg, ec] = helper::recvmsg(*sock_, true);
            if (ec) {
                throw std::system_error(ec, "recvmsg");
            }
            return msg.command() == command::ack;
        }
        return true;
    }

    void close()
    {
        helper::sendmsg(*sock_, command::close);
    }

  private:
    tbd::tcp_client sock_;
    bool once_flag_ = true;
};

class subscriber
{
    struct message
    {
        std::string topic;
        std::vector<uint8_t> data;
    };

  public:
    inline static const std::string DEFAULT_CLIENTID_FILE{".subscriber_id"};

    subscriber(std::string_view ipaddr, uint16_t port, std::string_view client_id = "")
        : sock_(ipaddr, port)
    {
        if (setup_client_id(client_id)) {
            save_client_id();
        }

        printf("subscriber id = %s\n", uuid_.c_str());
        sock_.set_nonblock(false);
    }

    void subscribe(std::string_view topic,
                   const std::function<bool(const subscriber::message& msg)>& callback)
    {
        sock_.connect();
        auto [_, lport] = sock_.local_endpoint();
        printf("subscriber: localport = %u\n", lport);
        helper::sendmsg(*sock_, command::subscribe, topic, uuid_.data(), 37);

        while (true) {
            auto [msg, ec] = helper::recvmsg(*sock_, true);
            if (ec) {
                printf("subscribe: recvmsg error: %s\n", ec.message().c_str());
                break;
            }
            if (msg.command() != command::push) {
                printf("subscribe: receive not push command (%u), disconnect\n",
                       (uint8_t)msg.command());
                break;
            }
            // printf("send_ack for msg %lu\n", msg.id());
            helper::send_ack(*sock_, msg.id());

            subscriber::message m;
            m.topic = msg.topic();
            m.data = std::move(msg.clone_data());
            if (!callback(m)) {
                break;
            }
        }
    }

    void unsubscribe()
    {
        helper::sendmsg(*sock_, command::unsubscribe);
    }

  private:
    tbd::tcp_client sock_;
    std::string uuid_;

    // @return save to file?
    bool setup_client_id(std::string_view id_arg)
    {
        uuid_t uuid_bin;

        // from argument
        if (!id_arg.empty()) {
            if (::uuid_parse(id_arg.data(), uuid_bin) < 0) {
                throw std::invalid_argument("invalid client id from argument. retry another one.");
            }
            uuid_ = id_arg;
            return true;
        }

        // from dot file
        std::ifstream ifs(DEFAULT_CLIENTID_FILE);
        if (ifs) {
            std::string id_file;
            std::getline(ifs, id_file);
            if (id_file.empty() || ::uuid_parse(id_file.data(), uuid_bin) < 0) {
                throw std::invalid_argument("invalid client id in file. remove it.");
            }
            uuid_ = std::move(id_file);
            return false;
        }
        ifs.close();

        // else: generate new one
        ::uuid_generate(uuid_bin);
        uuid_.reserve(37);
        uuid_.resize(36);
        ::uuid_unparse_lower(uuid_bin, uuid_.data());
        return true;
    }

    void save_client_id()
    {
        std::ofstream ofs(DEFAULT_CLIENTID_FILE);
        if (ofs) {
            ofs << uuid_ << "\n";
        }
    }
};

}  // namespace toymq

#endif
