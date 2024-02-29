#include <fstream>
#include <iostream>
#include <cstdio>

#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_stream.hh>

using namespace springtail;

int main(int argc, char* argv[])
{
    std::string file;

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("file,f", boost::program_options::value<std::string>(&file)->default_value("wal.log"), "WAL file to process");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    // init logging/backtrace
    springtail_init();

    PgMsgStreamReader reader(file);

    bool eob = false, eos = false;

    // consume messages from log until end of file
    while (!eos) {
        // read next message parsing all messages
        PgMsgPtr msg = reader.read_message(PgMsgStreamReader::ALL_MESSAGES, eos, eob);
        if (msg != nullptr) {
            // dump the message
            std::string s = pg_msg::dump_msg(*msg);
            std::cout << s;
        }
    }
}