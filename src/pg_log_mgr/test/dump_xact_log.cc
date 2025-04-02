#include <fmt/format.h>

#include <common/init.hh>

#include <pg_log_mgr/pg_xact_log_reader_mmap.hh>
#include <pg_log_mgr/pg_log_mgr.hh>
#include "common/init.hh"

using namespace springtail;
using namespace springtail::pg_log_mgr;

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

    PgXactLogReaderMmap reader(log_dir);
    reader.begin();

    bool has_more;
    do {
        std::cout << fmt::format("pgxid: {}, xid: {}",
                                 reader.get_pg_xid(), reader.get_xid()) << std::endl;
        has_more = reader.next();
    } while (has_more);

    springtail_shutdown();
}
