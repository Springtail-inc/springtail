#include <iostream>

#include <common/exception.hh>

void crash(const char *str) {
    std::cout << *str << std::endl;
}

void error() {
    throw st_common::Error();
}

int main() {
    // reference the global to force linker to resolve
    backward::sh.loaded();

    std::cout << "Throw!" << std::endl;
    try {
        error();
    } catch (st_common::Error &e) {
        e.print_trace();
    }

    std::cout << "Crash!" << std::endl;
    crash(nullptr);
    return 0;
}
