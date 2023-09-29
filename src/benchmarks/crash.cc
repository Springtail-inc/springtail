#include <iostream>

#include <common/exception.hh>
#include <common/logging.hh>

void crash(const char *str) {
    std::cout << *str << std::endl;
}

void error() {
    throw springtail::Error();
}

int main() {
    // reference the global to force linker to resolve
    backward::sh.loaded();
    springtail::init_logging();

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
