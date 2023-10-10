#include <fstream>
#include <iostream>
#include <thread>

#include <boost/program_options.hpp>

// springtail includes
#include <psql_cdc/pg_types.hh>
#include <psql_cdc/pg_repl_connection.hh>
#include <psql_cdc/pg_repl_msg.hh>


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

    if (!vm.count("password")) {
        std::cerr << "No password set\n";
        std::cerr << desc;
        return -1;
    }

    // create postgres connection
    springtail::PgReplConnection pg_conn(port, host, db_name, user_name, password, pub_name, slot_name);

    pg_conn.connect();
    std::cout << "Connecting to postgres server: " << host << std::endl;

    // create slot if need be
    create_slot = !pg_conn.checkSlotExists();

    if (create_slot) {
        std::cout << "Creating replication slot: " << slot_name << std::endl;
        pg_conn.createReplicationSlot(false,  // export
                                      false); // temporary
    }

    // start steaming
    pg_conn.startStreaming(springtail::INVALID_LSN);

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

    springtail::PgReplMsg msg(1); // init repl message w/proto vers 1

    while (true) {
        pg_conn.readData(data);

        std::cout << "Recevied data: " << data.length << "; msg length=" << data.msg_length
                  << "; msg offset=" << data.msg_offset << std::endl;

        if (data.length == 0) {
            // possible data has been consumed by keep alive
            continue;
        }

        // write out length
        char len_buf[4];
        springtail::sendint32(data.length, len_buf);

        out_fh.write(len_buf, 4);
        out_fh.write(data.buffer, data.length);
        out_fh.flush();

        if (dump_buffer) {
            // iterate through the messages
            msg.setBuffer(data.buffer, data.length);
            while (msg.hasNextMsg()) {
                const springtail::PgReplMsgDecoded &decoded_msg = msg.decodeNextMsg();
                std::string s = msg.dumpMsg(decoded_msg);
                std::cout << s;
            }
        }
    }

    return 0;
}