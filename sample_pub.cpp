#include "libclient.hpp"

#include <unistd.h>

using namespace std::string_literals;

int main()
{
    toymq::publisher pub("127.0.0.1", 55555);
    size_t i;
    for (i = 0; i < 100000; ++i) {
        pub.publish("foo.bar", "hello"s + std::to_string(i));
        //sleep(1);
    }
    printf("publisher: count = %lu\n", i);
    pub.publish("hoge", "not published");
    sleep(1);
    pub.publish("foo.exit", "bye!");
    pub.close();
}
