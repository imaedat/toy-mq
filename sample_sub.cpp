#include "libclient.hpp"

int main()
{
    toymq::subscriber sub("127.0.0.1", 55555);
    sub.subscribe("foo.*", [](const auto& msg) {
        printf("sub: topic=%s, data=%s\n", msg.topic.c_str(), (char*)msg.data.data());
        return msg.topic != "foo.exit";
    });
    sub.unsubscribe();
}
