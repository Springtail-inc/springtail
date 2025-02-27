#include <iostream>
#include <cstdio>

#include <boost/program_options.hpp>

// springtail includes
#include <common/common_init.hh>
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
    std::vector<ServiceRunner *> runners;
    springtail_init(runners, false);

    PgMsgStreamReader reader(file);

    bool eob = false, eos = false;

    // consume messages from log until end of file
    while (!eos) {
        // read next message parsing all messages
        PgMsgPtr msg = reader.read_message(reader.ALL_MESSAGES, eos, eob);
        if (msg != nullptr) {
            // dump the message
            std::string s = pg_msg::dump_msg(*msg);
            std::cout << s;
        }
    }
    springtail_shutdown();
}