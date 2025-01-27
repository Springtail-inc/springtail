#include <absl/log/check.h>

#include <fstream>
#include <iostream>
#include <proxy/logger.hh>

using namespace springtail::pg_proxy;

int
main(void)
{
    Logger logger("/tmp/springtail_binlog.txt", 1024 * 1024 * 5, 5);
    uint64_t data = 0x1234567ULL;

    std::string_view msg(reinterpret_cast<const char *>(&data), sizeof(data));
    std::vector<std::string_view> parts;
    parts.push_back(msg);

    logger.log(parts);
    logger.flush();

    // open the file /tmp/springtail_binlog.txt and read a binary uint64_t
    std::ifstream file("/tmp/springtail_binlog.txt", std::ios::binary);
    uint64_t value;
    if (file.read(reinterpret_cast<char *>(&value), sizeof(value))) {
        std::cout << fmt::format("Read value: {:#X}\n", value);
        CHECK_EQ(value, data);
    } else {
        std::cerr << "Failed to read value from file." << std::endl;
    }

    return 0;
}