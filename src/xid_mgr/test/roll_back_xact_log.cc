#include <iostream>
#include <string>
#include <boost/program_options.hpp>

#include <common/init.hh>

#include <xid_mgr/pg_xact_log_writer.hh>
#include <xid_mgr/pg_xact_log_reader.hh>

using namespace springtail;
using namespace springtail::xid_mgr;

int
main(int argc, char **argv)
{
    std::string base_dir;
    uint64_t db_id = 0;
    uint64_t xid = 0;
    bool archive = true;

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Options for rolling back xid log");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("path,p", po::value<std::string>(&base_dir)->required(), "Path for Xid logs storage directory");
    desc.add_options()("dbid,d", po::value<uint64_t>(&db_id)->default_value(1), "DB ID.");
    desc.add_options()("xid,x", po::value<uint64_t>(&xid)->required(), "Xid to set");
    desc.add_options()("archive,a", po::value<bool>(&archive)->default_value(true), "Archive removed logs flag");

    try {
        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);

        // check if we need to print the help message
        if (vm.count("help")) {
            std::cout << desc << std::endl;
            return 0;
        }
        po::notify(vm);
    } catch(std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    } catch(...) {
        std::cerr << "Unknown error!" << "\n";
        return 1;
    }

    std::vector<std::unique_ptr<ServiceRunner>> service_runners;
    service_runners.emplace_back(std::make_unique<ExceptionRunner>());

    springtail_init_custom(service_runners);

    std::filesystem::path db_path = base_dir + "/" + std::to_string(db_id);

    PgXactLogWriter::set_last_xid_in_storage(db_path, xid, archive);

    // verify xid logs
    PgXactLogReader reader(db_path);
    bool has_more = reader.begin();

    std::set<uint64_t> xids;
    std::set<uint32_t> pg_xids;
    uint64_t last_xid = 0;

    while (has_more) {
        uint64_t xid = reader.get_xid();
        uint32_t pg_xid = reader.get_pg_xid();

        CHECK(!xids.contains(xid)) << "Xid " << xid << " already exists in the set";
        xids.insert(xid);

        if (pg_xid != 0) {
            CHECK(!pg_xids.contains(pg_xid)) << "PgXid " << pg_xid << " already exists in the set";
            pg_xids.insert(pg_xid);
        }

        CHECK_GT(xid, last_xid) << "Xid " << xid << " is not greater than the last xid " << last_xid;
        last_xid = xid;

        has_more = reader.next();
    };


    springtail_shutdown();
    return 0;
}