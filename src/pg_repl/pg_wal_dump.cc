#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_connection.hh>
#include <pg_repl/pg_repl_msg.hh>


int main(int argc, char* argv[])
{
    std::string host;
    std::string db_name;
    std::string user_name;
    std::string password;
    std::string pub_name;
    std::string slot_name;
    std::filesystem::path outfile;

    bool dump_buffer = false;
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
        ("outfile,o", boost::program_options::value<std::filesystem::path>(&outfile)->default_value("wal.log"), "WAL output file")
        ("publication,b", boost::program_options::value<std::string>(&pub_name)->default_value("springtail"), "Publication name")
        ("dump,D", "Dump contents of buffer to stdout")
        ("slot,s", boost::program_options::value<std::string>(&slot_name)->default_value("springtail"), "Slot name; if none specified slot will be created");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (vm.count("dump")) {
        dump_buffer = true;
    }

    if (!vm.count("password") || password.empty()) {
        std::cerr << "No password set\n";
        std::cerr << desc;
        return -1;
    }

    // init logging/backtrace
    springtail::springtail_init();

    // create postgres connection
    springtail::PgReplConnection pg_conn(port, host, db_name, user_name, password, pub_name, slot_name);

    pg_conn.connect();
    std::cout << "Connecting to postgres server: " << host << std::endl;

    // create slot if need be
    create_slot = !pg_conn.check_slot_exists();

    if (create_slot) {
        std::cout << "Creating replication slot: " << slot_name << std::endl;
        pg_conn.create_replication_slot(false,  // export
                                        false); // temporary
    }

    // start steaming
    pg_conn.start_streaming(springtail::INVALID_LSN);

    int proto_version = pg_conn.get_protocol_version();

    // open output file
    std::fstream out_fh;
    out_fh.open(outfile, std::ios::out | std::ios::binary);
    if (!out_fh) {
        std::cerr << "Error opening output file: " << outfile << std::endl;
        return -1;
    }

    // loop through reading data and writing it to disk
    std::cout << "Connection and streaming have started.  Dumping data.\n";
    springtail::PgCopyData data;

    springtail::PgReplMsg msg(proto_version); // init repl message w/proto vers

    char int_buf[4];
    springtail::sendint32(proto_version, int_buf);
    out_fh.write(int_buf, 4);

    while (true) {
        pg_conn.read_data(data);

        std::cout << "Recevied data: " << data.length << "; msg length=" << data.msg_length
                  << "; msg offset=" << data.msg_offset << std::endl;

        if (data.length == 0) {
            // possible data has been consumed by keep alive
            continue;
        }

        // write out length if start of message
        if (data.msg_offset == 0) {
            springtail::sendint32(data.msg_length, int_buf);
            out_fh.write(int_buf, 4);
        }

        out_fh.write(data.buffer, data.length);
        out_fh.flush();

        // update LSNs
        if (data.msg_offset == 0) {
            pg_conn.set_last_flushed_LSN(data.starting_lsn);
        }

        if (data.msg_offset == data.msg_length) {
            pg_conn.set_last_flushed_LSN(data.ending_lsn);
        }

        if (dump_buffer && data.msg_offset == 0) {
            // iterate through the messages
            msg.set_buffer(data.buffer, data.length);
            while (msg.has_next_msg()) {
                const springtail::PgReplMsgDecoded &decoded_msg = msg.decode_next_msg();
                std::string s = msg.dump_msg(decoded_msg);
                std::cout << s;
            }
        }
    }

    return 0;
}
