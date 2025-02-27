#include <filesystem>
#include <iostream>

#include <boost/program_options.hpp>

// springtail includes
#include <common/common_init.hh>
#include <common/logging.hh>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_connection.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_stream.hh>

using namespace springtail;

int main(int argc, char* argv[])
{
    std::string host;
    std::string db_name;
    std::string user_name;
    std::string password;
    std::string pub_name;
    std::string slot_name;
    std::filesystem::path outfile;
    LSN_t lsn = INVALID_LSN;
    LSN_t restart_lsn;

    bool create_slot = false;
    int port;

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("host,H", po::value<std::string>(&host)->default_value("localhost"), "Hostname");
    desc.add_options()("port,p", po::value<int>(&port)->default_value(5432), "Port number");
    desc.add_options()("dbname,d", po::value<std::string>(&db_name)->default_value("springtail"), "DB database name");
    desc.add_options()("user,u", po::value<std::string>(&user_name)->default_value("springtail"), "DB user name");
    desc.add_options()("password,P", po::value<std::string>(&password)->default_value(""), "DB Password");
    desc.add_options()("outfile,o", po::value<std::filesystem::path>(&outfile)->default_value(std::filesystem::path("wal.log")), "WAL output file");
    desc.add_options()("publication,b", po::value<std::string>(&pub_name)->default_value("springtail"), "Publication name");
    desc.add_options()("slot,s", po::value<std::string>(&slot_name)->default_value("springtail"), "Slot name; if none specified slot will be created");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (!vm.count("password") || password.empty()) {
        std::cerr << "No password set\n";
        std::cerr << desc;
        return -1;
    }

    std::vector<ServiceRunner *> runners;
    // init logging/backtrace
    springtail_init(runners, std::nullopt, std::nullopt, LOG_PG_REPL);

    // create postgres connection
    PgReplConnection pg_conn(port, host, db_name, user_name, password, pub_name, slot_name);

    pg_conn.connect();
    SPDLOG_INFO("Connecting to postgres server: host={}\n", host);

    // create slot if need be; retrieve restart LSN and last flushed lsn
    create_slot = !pg_conn.check_slot_exists(restart_lsn, lsn);

    if (create_slot) {
        SPDLOG_INFO("Creating replication slot: name={}\n", slot_name);
        pg_conn.create_replication_slot(false,  // export
                                        false); // temporary
    }

    // start steaming
    pg_conn.start_streaming(lsn);

    // open output file
    PgMsgStreamWriter writer(outfile);

    // loop through reading data and writing it to disk
    SPDLOG_INFO("Connection and streaming have started @ LSN={}.  Dumping data.\n", lsn);
    PgCopyData data;
    bool skip = true;

    while (true) {
        pg_conn.read_data(data);

        SPDLOG_INFO("Recevied data: data len={}, msg len={}, msg offset={}\n",
                     data.length, data.msg_length, data.msg_offset);
        SPDLOG_INFO("  - start LSN={}, end LSN={}\n",
                    data.starting_lsn, data.ending_lsn);

        writer.write_message(data);

        // update LSNs
        if (data.msg_offset + data.length == data.msg_length) {
            if (true || !skip) {
                SPDLOG_INFO("Setting last flushed LSN: {}\n", data.ending_lsn);
                pg_conn.set_last_flushed_LSN(data.ending_lsn);
            }
            skip = !skip;
        }
    }

    springtail_shutdown();
    return 0;
}
