#include <filesystem>
#include <iostream>

#include <boost/program_options.hpp>

// springtail includes
#include <common/init.hh>
#include <common/logging.hh>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_connection.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_stream.hh>
#include <pg_repl/libpq_connection.hh>

using namespace springtail;

LibPqConnectionPtr
write_sql_data(const std::string &db_host,
               const std::string &db_name,
               const std::string &db_user,
               const std::string &db_pass,
               const int db_port)
{
    // generate some data
    LibPqConnectionPtr data_conn = std::make_shared<LibPqConnection>();
    data_conn->connect(db_host, db_name, db_user, db_pass, db_port, false);

    LibPqConnectionPtr data_conn2 = std::make_shared<LibPqConnection>();
    data_conn2->connect(db_host, db_name, db_user, db_pass, db_port, false);

    // create table if not exists
    std::string create_sql = "CREATE TABLE IF NOT EXISTS public.test_reconnect (id SERIAL PRIMARY KEY, data INTEGER);";
    data_conn->exec(create_sql);

    create_sql = "CREATE TABLE IF NOT EXISTS public.test_reconnect2 (id SERIAL PRIMARY KEY, data INTEGER);";
    data_conn2->exec(create_sql);

    // start long running transaction on another connection
    data_conn2->start_transaction();
    data_conn2->exec("INSERT INTO public.test_reconnect2 (data) SELECT generate_series(1, 2) AS n;");

    for (int i = 0; i < 3; i++) {
        data_conn->start_transaction();
        data_conn->exec("INSERT INTO public.test_reconnect (data) SELECT generate_series(1, 2) AS n;");
        data_conn->end_transaction();
    }

    data_conn->disconnect();

    return data_conn2;
}

void
complete_long_running_tx(LibPqConnectionPtr &data_conn2)
{
    data_conn2->exec("INSERT INTO public.test_reconnect2 (data) SELECT generate_series(1,2) AS n;");
    data_conn2->end_transaction();
    data_conn2->disconnect();
}

int
main(int argc, char* argv[])
{
    std::string host;
    std::string db_name;
    std::string user_name;
    std::string password;
    std::string pub_name;
    std::string slot_name;
    std::filesystem::path outfile;
    LSN_t lsn = INVALID_LSN;
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

    // init logging/backtrace
    springtail_init(false, std::nullopt, LOG_PG_REPL);

    // create postgres connection
    PgReplConnection pg_conn(port, host, db_name, user_name, password, pub_name, slot_name);

    pg_conn.connect();
    LOG_INFO("Connecting to postgres server: host={}\n", host);

    pg_conn.drop_publication();
    pg_conn.drop_replication_slot();

    // check and create publication as well if need be
    pg_conn.create_publication();
    pg_conn.create_replication_slot();

    // start steaming
    LOG_INFO("Starting streaming: lsn={}\n", lsn);
    pg_conn.start_streaming(lsn, true);

    // open output file
    PgMsgStreamWriter writer(outfile);

    // write some data to the database to generate some WAL
    auto long_tx_conn = write_sql_data(host, db_name, user_name, password, port);

    // loop through reading data and writing it to disk
    LOG_INFO("Connection and streaming have started @ LSN={}.", lsn);
    PgCopyData data;
    char msg_type = 0;

    while (true) {
        pg_conn.read_data(data);
        if (data.length == 0) {
            continue;
        }

        LOG_INFO("Received data: data len={}, msg len={}, msg offset={}, writer_offset={}",
                     data.length, data.msg_length, data.msg_offset, writer.offset());
        LOG_INFO("  - start LSN={}, end LSN={}",
                    data.starting_lsn, data.ending_lsn);

        if (data.msg_offset == 0) {
            // write the header
            PgMsgStreamHeader header(data.msg_length, INVALID_LSN);
            writer.write_header(header);

            // first byte is the message type
            msg_type = data.buffer[0];
        }
        writer.write_message(data);

        if (msg_type == pg_msg::MSG_COMMIT) {
            CHECK(data.msg_length >= 18);
            // skip first msg type byte
            auto flags = recvint8(&data.buffer[1]);
            CHECK_EQ(flags, 0);
            auto commit_lsn = recvint64(&data.buffer[2]);
            auto xact_lsn = recvint64(&data.buffer[10]);

            lsn = commit_lsn + 1; // move past the commit lsn
            LOG_INFO("  - COMMIT lsn={} xact_lsn={}\n", commit_lsn, xact_lsn);

            // stop at first commit
            break;
        }
    }

    // complete the long running transaction
    complete_long_running_tx(long_tx_conn);

    // reconnect and continue streaming from lsn from commit
    LOG_INFO("Reconnecting to postgres server: restarting from LSN={}", lsn);
    pg_conn.reconnect(lsn);
    auto reconnect_offset = writer.offset();

    msg_type = 0;
    int commit_count = 0;
    LSN_t last_commit_lsn = lsn - 1;

    // continue processing data until we get three more commits
    while (commit_count < 3) {
        pg_conn.read_data(data);
        if (data.length == 0) {
            continue;
        }

        LOG_INFO("Received data: data len={}, msg len={}, msg offset={}, writer_offset={}",
                     data.length, data.msg_length, data.msg_offset, writer.offset());
        LOG_INFO("  - start LSN={}, end LSN={}, last_commit_lsn={}",
                    data.starting_lsn, data.ending_lsn, last_commit_lsn);

        if (data.msg_offset == 0) {
            // write the header
            PgMsgStreamHeader header(data.msg_length, last_commit_lsn);
            writer.write_header(header);

            msg_type = data.buffer[0];

            LOG_INFO("  - writing message type={}", msg_type);
        }

        writer.write_message(data);

        if (msg_type == pg_msg::MSG_COMMIT && data.msg_offset + data.length >= data.msg_length) {
            commit_count++;

            CHECK(data.msg_length >= 10);
            last_commit_lsn = recvint64(&data.buffer[2]);
        }
    }

    writer.close();

    // Go through the log file and verify
    PgMsgStreamReader reader({}, outfile, 0);
    bool eos = false;

    std::set<LSN_t> begin_lsns;
    std::set<LSN_t> commit_lsns;

    std::cout << std::format("\nVerifying log file: {}\n", outfile.string());
    while (!eos) {

        PgMsgPtr msg = reader.read_message(reader.ALL_MESSAGES, eos);
        if (msg == nullptr) {
            continue;
        }

        auto header = reader.current_header();

        if (header.header_offset == reconnect_offset) {
            std::cout << std::format("\n>>>>>>>>> Reached reconnect offset: {}, restarted with: {}\n\n", reconnect_offset, lsn);
        }

        std::cout << std::format("Xlog {}, xlog_end_offset: {}\n",
                                 header.to_string(),
                                 header.msg_length + header.header_offset + PgMsgStreamHeader::SIZE);

        // dump the message
        std::string msg_str = pg_msg::dump_msg(*msg);
        std::cout << msg_str;

        if (msg->msg_type == PgMsgEnum::BEGIN) {
            auto begin_msg = std::get<PgMsgBegin>(msg->msg);
            if (begin_lsns.contains(begin_msg.xact_lsn)) {
                std::cerr << "Duplicate BEGIN LSN found: " << begin_msg.xact_lsn << "\n";
                exit(-1);
            }
            begin_lsns.insert(begin_msg.xact_lsn);
        } else if (msg->msg_type == PgMsgEnum::COMMIT) {
            auto commit_msg = std::get<PgMsgCommit>(msg->msg);
            if (commit_lsns.contains(commit_msg.commit_lsn)) {
                std::cerr << "Duplicate COMMIT LSN found: " << commit_msg.commit_lsn << "\n";
                exit(-1);
            }
            commit_lsns.insert(commit_msg.commit_lsn);
        }
    }

    pg_conn.close();
    springtail_shutdown();

    return 0;
}
