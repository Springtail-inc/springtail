#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>
#include <iostream>

class CrashError : public springtail::Error {
public:
    CrashError() {}
    CrashError(const std::string &error) : springtail::Error(error) {}
};

void
crash(const char *str)
{
    std::cout << *str << std::endl;
}

void
error()
{
    throw CrashError();
}

int
main()
{
    // initialize the common framework
    springtail::springtail_init();

    std::cout << "Throw!" << std::endl;
    try {
        error();
    } catch (springtail::Error &e) {
        e.log_backtrace();
        SPDLOG_ERROR("Caught an error!");
    }

    std::cout << "Crash!" << std::endl;
    crash(nullptr);
    return 0;
}
