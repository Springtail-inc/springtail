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

    std::string format_str = fmt::format("pgxid: {{:>{}}} | xid: {{:>{}}} | real_commit: {{}}",
                                         16, 16);
    bool has_more = reader.begin();

    std::set<uint64_t> xid_set;
    std::set<uint32_t> pg_xid_set;

    uint64_t last_xid = 0;
    while (has_more) {
        auto pg_xid = reader.get_pg_xid();
        auto xid = reader.get_xid();
        std::cout << fmt::format(fmt::runtime(format_str.c_str()),
                                 pg_xid, xid, reader.get_real_commit()) << std::endl;

        // Check if xid is greater than the last xid
        if (xid <= last_xid) {
            std::cerr << fmt::format("Error: xid {} is not greater than last xid {}", xid, last_xid) << std::endl;
            std::cerr << "Current file: " << reader.get_current_file() << std::endl;
            return 1;
        }
        last_xid = xid;

        // Check if xid exists in the set
        if (!xid_set.insert(xid).second) {
            std::cerr << fmt::format("Duplicate xid found: {}, pg_xid: {}", xid, pg_xid) << std::endl;
            std::cerr << "Current file: " << reader.get_current_file() << std::endl;
            //return 1;
        }

        // Check if pg_xid exists in the set
        if (!pg_xid_set.insert(pg_xid).second) {
            std::cerr << fmt::format("Duplicate pg_xid found: {}, xid: {}", pg_xid, xid) << std::endl;
            std::cerr << "Current file: " << reader.get_current_file() << std::endl;
            //return 1;
        }

        has_more = reader.next();
    };

    springtail_shutdown();
}
