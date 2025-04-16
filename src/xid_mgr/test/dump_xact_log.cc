#include <fmt/format.h>

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
    reader.begin(true);

    bool has_more;
    do {
        std::cout << fmt::format("pgxid: {}, xid: {}, real_commit: {}",
                                 reader.get_pg_xid(), reader.get_xid(), reader.get_real_commit()) << std::endl;
        has_more = reader.next();
    } while (has_more);

    springtail_shutdown();
}
