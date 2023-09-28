#include <iostream>

#include <backward.hpp>
namespace backward {
    backward::SignalHandling sh;
}

void crash(const char *str) {
    std::cout << *str << std::endl;
}

int main() {
    std::cout << "Crash!" << std::endl;
    crash(nullptr);
    return 0;
}
