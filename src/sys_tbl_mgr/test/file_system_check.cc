#include <common/init.hh>

#include <test/file_system_check.hh>
using namespace springtail;

int
main(int argc, char *argv[])
{
    // no logging
    springtail_init(false, std::nullopt, LOG_NONE);

    std::shared_ptr<test::FSCheck> fs_check = std::make_shared<test::FSCheck>();
    fs_check->check_dbs();
    fs_check.reset();

    springtail_shutdown();
}