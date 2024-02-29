#include <filesystem>
#include <iostream>

#include <boost/program_options.hpp>

#include <common/common.hh>

#include <pg_repl/pg_msg_log_gen.hh>

using namespace springtail;

int main(int argc, char **argv)
{
    std::string input_file, output_file;
    bool dump;

    // Declare the supported options
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help", "Generate a Postgres log file from a JSON input file.")
        ("input,i", boost::program_options::value<std::string>(&input_file), "json input file")
        ("output,o", boost::program_options::value<std::string>(&output_file), "output file")
        ("dump,d", boost::program_options::bool_switch(&dump), "dump");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }

    if (!vm.count("input") || !vm.count("output")) {
        std::cerr << "Input and output files are required\n";
        std::cerr << desc;
        return -1;
    }

    springtail_init();

    PgLogGenJson log_gen(output_file);
    log_gen.parse_commands(input_file);

    if (dump) {
        PgMsgLogGen::dump_file(output_file);
    }

    return 0;
}