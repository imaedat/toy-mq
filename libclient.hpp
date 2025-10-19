#ifndef MQ_LIB_CLIENT_HPP_
#define MQ_LIB_CLIENT_HPP_

#include "proto.hpp"

#include "socket.hpp"

namespace toymq {

class publisher
{
  public:
    publisher(std::string_view ipaddr, uint16_t port)
        : sock_(ipaddr, port)
    {
    }

    void publish(std::string_view topic, std::string_view msg)
    {
        publish(topic, msg.data(), msg.size());
    }

    void publish(std::string_view topic, const void* data, size_t size)
    {
        sock_.connect();
        auto ec = helper::sendmsg(sock_.native_handle(), command::publish, topic, data, size);
        if (ec) {
            throw std::system_error(ec);
        }
    }

    void close()
    {
        helper::sendmsg(sock_.native_handle(), command::close);
    }

  private:
    tbd::tcp_client sock_;
};

class subscriber
{
    struct message
    {
        std::string topic;
        std::vector<uint8_t> data;
    };

  public:
    subscriber(std::string_view ipaddr, uint16_t port)
        : sock_(ipaddr, port)
    {
    }

    void subscribe(std::string_view topic,
                   const std::function<bool(const subscriber::message& msg)>& callback)
    {
        sock_.connect();
        helper::sendmsg(sock_.native_handle(), command::subscribe, topic);

        sock_.set_nonblock(false);
        while (true) {
            auto [msg, ec] = helper::recvmsg(sock_.native_handle());
            if (ec) {
                printf("subscribe: receive error: %s\n", ec.message().c_str());
                break;
            }
            helper::send_ack(sock_.native_handle(), msg.id());

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
        helper::sendmsg(sock_.native_handle(), command::unsubscribe);
    }

  private:
    tbd::tcp_client sock_;
};

}  // namespace toymq

#endif
