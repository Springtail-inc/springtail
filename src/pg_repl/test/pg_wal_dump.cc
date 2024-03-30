#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
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
    LSN_t lsn = INVALID_LSN, restart_lsn;

    bool create_slot = false;
    int port;

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("host,H", boost::program_options::value<std::string>(&host)->default_value("localhost"), "Hostname")
        ("port,p", boost::program_options::value<int>(&port)->default_value(5432), "Port number")
        ("dbname,d", boost::program_options::value<std::string>(&db_name)->default_value("springtail"), "DB database name")
        ("user,u", boost::program_options::value<std::string>(&user_name)->default_value("springtail"), "DB user name")
        ("password,P", boost::program_options::value<std::string>(&password)->default_value(""), "DB Password")
        ("outfile,o", boost::program_options::value<std::filesystem::path>(&outfile)->default_value(std::filesystem::path("wal.log")), "WAL output file")
        ("publication,b", boost::program_options::value<std::string>(&pub_name)->default_value("springtail"), "Publication name")
        ("slot,s", boost::program_options::value<std::string>(&slot_name)->default_value("springtail"), "Slot name; if none specified slot will be created");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

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

    // init logging/backtrace
    springtail_init(LOG_PG_REPL);

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

    return 0;
}
