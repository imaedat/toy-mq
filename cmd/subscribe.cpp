#include "../libclient.hpp"

#include <signal.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

namespace {
void signal_handler(int)
{
    // nop
}

void usage()
{
    std::cerr
        << "usage: subscribe [-k] [-a ADDR(127.0.0.1)] [-p PORT(55555)] -t TOPIC callback command ..."
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
    bool keep = false;

    while ((opt = ::getopt(argc, argv, "ka:p:t:")) != -1) {
        switch (opt) {
        case 'k':
            keep = true;
            break;
        case 'a':
            addr = optarg;
            break;
        case 'p':
            port = ::atoi(optarg);
            break;
        case 't':
            topic = optarg;
            break;
        default:
            usage();
            break;
        }
    }

    if (!topic) {
        usage();
    }

    struct sigaction sa = {};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);

    toymq::subscriber sub(addr, port);
    try {
        sub.subscribe(topic, [&argv, keep](const auto& msg) {
            char buf[msg.data.size() + 1] = {};
            ::memcpy(buf, msg.data.data(), msg.data.size());

            std::string cmd{"env TOYMQ_MSG=\""};
            cmd.append(buf).append("\" sh -c \"");
            for (int i = optind; argv[i] != nullptr; ++i) {
                cmd.append(" ").append(argv[i]);
            }
            cmd.append("\"");
            auto ec = ::system(cmd.c_str());
            (void)ec;
            return keep;
        });

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    try {
        sub.unsubscribe();
    } catch (...) {
        //
    }
}
