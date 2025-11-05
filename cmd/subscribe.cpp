#include "../libclient.hpp"

#include <signal.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

#include "../../cpp-libs/cmdopt.hpp"

namespace {
void signal_handler(int)
{
    // nop
}
}  // namespace

int main(int argc, char* argv[])
{
    tbd::cmdopt opt(argv[0]);
    opt.optional('a', "addr", "127.0.0.1", "broker address");
    opt.optional('p', "port", 55555, "broker port");
    opt.mandatory('t', "topic", "topic to subscribe");
    opt.flag('k', "keep", "keep subscribing. in default, unsubscribe after subscribe one message");
    opt.flag('h', "help", "show this message");

    auto err = opt.try_parse(argc, argv);
    if (opt.exists("help")) {
        std::cerr << opt.usage();
        ::exit(EXIT_SUCCESS);
    }
    if (err) {
        std::cerr << *err << std::endl;
        std::cerr << opt.usage();
        ::exit(EXIT_FAILURE);
    }

    struct sigaction sa = {};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);

    auto keep = opt.exists("keep");
    toymq::subscriber sub(opt.get<std::string>("addr"), opt.get<uint16_t>("port"));
    try {
        sub.subscribe(opt.get<std::string>("topic"), [&argv, &opt](const auto& msg) {
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
            return opt.exists("keep");
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
