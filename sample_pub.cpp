#include "libclient.hpp"

#include <unistd.h>

using namespace std::string_literals;

int main()
{
    toymq::publisher pub("127.0.0.1", 55555);
    size_t i;
    for (i = 0; /*i < 100000*/; ++i) {
        try {
            pub.publish("foo.bar", "hello"s + std::to_string(i));
        } catch (const std::system_error& e) {
            printf("publish error: %s\n", strerror(e.code().value()));
            if (e.code().value() == EAGAIN || e.code().value() == EWOULDBLOCK) {
                sleep(1);
            } else {
                exit(9);
            }
        }
    }
    printf("publisher: count = %lu\n", i);
    pub.publish("hoge", "not published");
    sleep(1);
    pub.publish("foo.exit", "bye!");
    pub.close();
}
