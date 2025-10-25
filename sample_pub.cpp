#include "libclient.hpp"

#include <unistd.h>

#include <memory>

using namespace std::string_literals;

int main(int argc, char*[])
{
    bool require_ack = argc > 1;
    auto pub = std::make_unique<toymq::publisher>("127.0.0.1", 55555);
    size_t count = 0;
    size_t decimation = require_ack ? 10000 : 100000;
    unsigned wait_ms = 100;
    while (true) {
        try {
            if (pub->publish("foo.bar", "hello"s + std::to_string(count), require_ack)) {
                if (++count % decimation == 0) {
                    printf("publisher: publish %lu messages\n", count);
                }
                wait_ms = 100;
            } else {
                printf("publisher: not ack repy, wait\n");
                usleep(wait_ms * 1000);
                wait_ms = std::min(wait_ms << 1, 10U * 1000);
            }
        } catch (const std::system_error& e) {
            printf("publish error: %s\n", e.what());
            if (e.code().value() == ENOENT || e.code().value() == ECONNREFUSED) {
                exit(9);
            }
            pub.reset();
            pub = std::make_unique<toymq::publisher>("127.0.0.1", 55555);
        }
    }

    // printf("publisher: count = %lu\n", count);
    // pub->publish("hoge", "not published");
    // usleep(500 * 1000);
    // pub->publish("foo.exit", "bye!");
    // pub->close();
}
