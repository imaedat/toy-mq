#include "../libclient.hpp"

#include <unistd.h>

#include <cstdlib>
#include <iostream>

namespace {
void usage()
{
    std::cerr << "usage: publish [-a ADDR(127.0.0.1)] [-p PORT(55555)] -t TOPIC [-m MESSAGE(empty)]"
              << std::endl;
    ::exit(EXIT_FAILURE);
}
}  // namespace

int main(int argc, char* argv[])
{
    int opt;
    const char* addr = "127.0.0.1";
    uint16_t port = 55555;
    const char* topic = nullptr;
    const char* message = "";

    while ((opt = ::getopt(argc, argv, "a:p:t:m:")) != -1) {
        switch (opt) {
        case 'a':
            addr = optarg;
            break;
        case 'p':
            port = ::atoi(optarg);
            break;
        case 't':
            topic = optarg;
            break;
        case 'm':
            message = optarg;
            break;
        default:
            usage();
            break;
        }
    }

    if (!topic) {
        usage();
    }

    toymq::publisher pub(addr, port);
    pub.publish_oneshot(topic, message);
}
