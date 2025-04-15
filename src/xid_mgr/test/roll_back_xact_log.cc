#include <iostream>
#include <string>
#include <boost/program_options.hpp>

#include <common/init.hh>

#include <xid_mgr/pg_xact_log_writer.hh>

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

    int ret = 0;
    if (!PgXactLogWriter::set_last_xid_in_storage(db_path, xid, archive)) {
        std::cout << "Failed to roll xid back to " << xid << std::endl;
        ret = 1;
    }

    springtail_shutdown();
    return ret;
}