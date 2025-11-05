#include "../libclient.hpp"

#include <unistd.h>

#include <cstdlib>
#include <iostream>

#include "../../cpp-libs/cmdopt.hpp"

int main(int argc, char* argv[])
{
    tbd::cmdopt opt(argv[0]);
    opt.optional('a', "addr", "127.0.0.1", "broker address");
    opt.optional('p', "port", 55555, "broker port");
    opt.mandatory('t', "topic", "topic to publish");
    opt.optional('m', "message", "", "message to publish");
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

    toymq::publisher pub(opt.get<std::string>("addr"), opt.get<uint16_t>("port"));
    pub.publish_oneshot(opt.get<std::string>("topic"), opt.get<std::string>("message"));
}
