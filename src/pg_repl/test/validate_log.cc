#include <iostream>
#include <cstdio>

#include <boost/program_options.hpp>

// springtail includes
#include <common/init.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_stream.hh>

using namespace springtail;

int main(int argc, char* argv[])
{
    std::string file;

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("file,f", po::value<std::string>(&file)->default_value("wal.log"), "WAL file to process");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (file.empty() || !std::filesystem::exists(std::filesystem::path(file))) {
        std::cerr << "No file specified" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    // init logging/backtrace
    springtail_init(std::nullopt, false, std::nullopt, LOG_PG_REPL);

    uint64_t last_lsn = PgMsgStreamReader::scan_log(file);

    std::cout << "Found Last LSN: " << last_lsn << std::endl;
    springtail_shutdown();
}
