#include <iostream>

#include <common/exception.hh>
#include <common/logging.hh>
#include <common/common.hh>

void crash(const char *str) {
    std::cout << *str << std::endl;
}

void error() {
    throw springtail::Error();
}

int main() {
    // initialize the common framework
    springtail::springtail_init();

    std::cout << "Throw!" << std::endl;
    try {
        error();
    } catch (springtail::Error &e) {
        e.print_trace();
        SPDLOG_ERROR("Caught an error!");
    }

    std::cout << "Crash!" << std::endl;
    crash(nullptr);
    return 0;
}
