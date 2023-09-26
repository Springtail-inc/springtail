#include <fstream>
#include <iostream>
#include <thread>

#include <boost/program_options.hpp>

// springtail includes
#include <psql_cdc/pg_types.hh>
#include <psql_cdc/pg_repl_connection.hh>

static const std::string SLOT_NAME="springtail";

int main(int argc, char* argv[])
{
    std::string host;
    std::string db_name;
    std::string user_name;
    std::string password;
    std::string slot_name;
    std::filesystem::path outfile;

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
        ("slot,s", boost::program_options::value<std::string>(&slot_name)->default_value(""), "Slot name; if none specified slot will be created");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    // if no slot specified use default, but set create slot to true
    if (slot_name.empty()) {
        create_slot = true;
        slot_name = SLOT_NAME;
    }

    // create postgres connection
    PgReplConnection pg_conn(port, host, db_name, user_name, password, slot_name);

    std::cout << "Connecting to postgres server: " << host;
    int r = pg_conn.connect();
    if (r != 0) {
        return r;
    }

    // create slot if need be
    if (create_slot) {
        std::cout << "Creating replication slot: " << slot_name;
        r = pg_conn.createReplicationSlot(PgReplConnection::OutputPlugin::PGOUTPUT,
                                          false, false);
        if (r != 0) {
            return r;
        }
    }

    // check that the slot exists
    if (!pg_conn.checkSlotExists()) {
        std::cerr << "Error: replication slot not found: " << slot_name;
        return -1;
    }

    // start steaming
    r = pg_conn.startStreaming(INVALID_LSN);
    if (r < 0) {
        std::cerr << "Error: start streaming failed";
        return -1;
    }

    // open output file
    std::fstream out_fh;
    out_fh.open(outfile, std::ios::out | std::ios::binary);
    if (!out_fh) {
        std::cerr << "Error opening output file: " << outfile;
        return -1;
    }

    // loop through reading data and writing it to disk
    PgCopyData data;
    while ((r = pg_conn.readData(data)) != 0) {
        std::cout << "Recevied data: " << data.length << " B";

        // write out length
        char len_buf[4];
        sendint32(data.length, len_buf);

        out_fh.write(len_buf, 4);
        out_fh.write(data.buffer, data.length);
        out_fh.flush();
    }

    if (r < 0) {
        std::cerr << "Error reading copy data";
    }

    return 0;
}