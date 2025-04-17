#include <fmt/format.h>
#include <string>

#include <common/init.hh>

#include <xid_mgr/pg_xact_log_reader.hh>

using namespace springtail;
using namespace springtail::xid_mgr;

int main(int argc, char *argv[])
{
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <log_dir>" << std::endl;
        return 1;
    }

    std::filesystem::path log_dir = argv[1];

    std::vector<std::unique_ptr<ServiceRunner>> service_runners;
    service_runners.emplace_back(std::make_unique<ExceptionRunner>());

    springtail_init_custom(service_runners);

    PgXactLogReader reader(log_dir);
    bool has_more = reader.begin(true);
    uint32_t pg_xid_max_len = 0;
    uint32_t xid_max_len = 0;

    while (has_more) {
        std::string pg_xid_str = std::to_string(reader.get_pg_xid());
        if (pg_xid_str.length() > pg_xid_max_len) {
            pg_xid_max_len = pg_xid_str.length();
        }
        std::string xid_str = std::to_string(reader.get_xid());
        if (xid_str.length() > xid_max_len) {
            xid_max_len = xid_str.length();
        }
        has_more = reader.next();
    };

    std::string format_str = fmt::format("pgxid: {{:>{}}} | xid: {{:>{}}} | real_commit: {{}}",
                                         pg_xid_max_len + 1, xid_max_len + 1);
    has_more = reader.begin(true);

    while (has_more) {
        std::cout << fmt::format(fmt::runtime(format_str.c_str()),
                                 reader.get_pg_xid(), reader.get_xid(), reader.get_real_commit()) << std::endl;
        has_more = reader.next();
    };

    springtail_shutdown();
}
