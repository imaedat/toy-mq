#ifndef TOYMQ_PROTO_HPP_
#define TOYMQ_PROTO_HPP_

#include <arpa/inet.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <system_error>
#include <vector>

// XXX
// #include "hexdump.h"

namespace toymq {

static constexpr uint8_t VERSION = 1;

enum class command : uint8_t
{
    void_ = 0,
    conn_pub,
    conn_sub,
    ack,
    nack,
    publish,
    publish_ack,
    publish_oneshot,
    subscribe,
    unsubscribe,
    push,
    close,
};

struct header
{
    uint8_t version;
    uint8_t command;
    uint16_t length;
    uint8_t payload[];
} __attribute__((packed));

struct message
{
    std::vector<uint8_t> buf;

    message()
        : buf(sizeof(header), 0)
    {
    }

    message(const message&) = default;
    message& operator=(const message&) = delete;

    message(message&&) = default;
    message& operator=(message&&) = default;

    header* hdr() const noexcept
    {
        return (header*)buf.data();
    }

    void resize(uint16_t new_len)
    {
        buf.resize(new_len, 0);
        hdr()->length = htons(new_len);
    }

    uint16_t length() const noexcept
    {
        return ntohs(hdr()->length);
    }

    uint8_t* payload() const noexcept
    {
        return hdr()->payload;
    }

    uint16_t payload_size() const noexcept
    {
        return length() - sizeof(header);
    }

    toymq::command command() const noexcept
    {
        return (toymq::command)hdr()->command;
    }

    size_t topic_size() const noexcept
    {
        return payload()[0];
    }

    std::string_view topic() const noexcept
    {
        return {(char*)&payload()[1], topic_size() - 1};
    }

    uint64_t id() const noexcept
    {
        return be64toh(*(uint64_t*)(buf.data() + length() - sizeof(uint64_t)));
    }

    void id(uint64_t new_id) noexcept
    {
        *(uint64_t*)(buf.data() + length() - sizeof(uint64_t)) = htobe64(new_id);
    }

    uint16_t data_size() const noexcept
    {
        return ntohs(*(uint16_t*)(payload() + sizeof(uint8_t) + topic_size()));
    }

    uint8_t* data() const noexcept
    {
        return payload() + sizeof(uint8_t) + topic_size() + sizeof(uint16_t);
    }

    std::vector<uint8_t> clone_data() const
    {
        auto begin =
            buf.begin() + sizeof(header) + sizeof(uint8_t) + topic_size() + sizeof(uint16_t);
        auto end = begin + data_size();
        return {begin, end};
    }
};

namespace helper {
int recv_exact(int sockfd, void* buf, size_t size, bool retry)
{
    auto* ptr = (uint8_t*)buf;
    long want = size;
    while (want > 0) {
        auto nread = ::read(sockfd, ptr, want);
        if (nread < 0) {
            if (errno == EINTR) {
                continue;
            }
            if ((errno == EAGAIN || errno == EWOULDBLOCK) && retry) {
                continue;
            }
            return errno;
        }
        if (nread == 0) {
            return ENOENT;
        }
        ptr += nread;
        want -= nread;
    }
    return 0;  // SUCCESS
}

std::pair<message, std::error_code> recvmsg(int sockfd)
{
    message msg;
    if (auto err = recv_exact(sockfd, msg.hdr(), sizeof(header), false)) {
        return {std::move(msg), std::error_code(err, std::generic_category())};
    }

    std::error_code ec;
    if (msg.length() > sizeof(header)) {
        msg.resize(msg.length());
        if (auto err = recv_exact(sockfd, msg.payload(), msg.payload_size(), true)) {
            ec = std::error_code(err, std::generic_category());
        }
    }

    return {std::move(msg), ec};
}

header* fill_header(const std::vector<uint8_t>& msg, command c, uint16_t size)
{
    auto* hdr = (header*)msg.data();
    hdr->version = VERSION;
    hdr->command = (uint8_t)c;
    hdr->length = htons(size);
    return hdr;
}

std::error_code sendmsg(int sockfd, command c, std::string_view topic = "",
                        const void* data = nullptr, size_t datasz = 0)
{
    auto msgsz = sizeof(header);
    if (!topic.empty()) {
        msgsz += sizeof(uint8_t) + topic.size() + 1;
    }
    if (data) {
        msgsz += sizeof(uint16_t) + datasz;
    }

    std::vector<uint8_t> msg(msgsz);
    auto* hdr = fill_header(msg, c, msgsz);

    if (!topic.empty()) {
        hdr->payload[0] = topic.size() + 1;
        ::memcpy(&hdr->payload[1], topic.data(), topic.size());
    }

    if (data) {
        auto* l = hdr->payload + sizeof(uint8_t) + topic.size() + 1;
        *(uint16_t*)l = htons(datasz);
        auto* d = l + sizeof(uint16_t);
        ::memcpy(d, data, datasz);
    }

    std::error_code ec;
    if (::send(sockfd, hdr, msgsz, MSG_NOSIGNAL) < 0) {
        ec = std::error_code(errno, std::generic_category());
    }
    return ec;
}

std::error_code send_ack(int sockfd, uint64_t msgid)
{
    auto msgsz = sizeof(header) + sizeof(uint64_t);
    std::vector<uint8_t> msg(msgsz);
    auto* hdr = fill_header(msg, command::ack, msgsz);
    *(uint64_t*)hdr->payload = htobe64(msgid);

    std::error_code ec;
    if (::send(sockfd, hdr, msgsz, MSG_NOSIGNAL) < 0) {
        ec = std::error_code(errno, std::generic_category());
    }
    return ec;
}
}  // namespace helper

}  // namespace toymq

#endif
