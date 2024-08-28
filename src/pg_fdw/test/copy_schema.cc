#include <filesystem>
#include <map>

#include <fmt/format.h>
#include <boost/program_options.hpp>

#include <common/common.hh>
#include <common/constants.hh>
#include <common/logging.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <pg_repl/pg_copy_table.hh>
#include <pg_repl/libpq_connection.hh>

#include <storage/table.hh>
#include <storage/table_mgr.hh>
#include <storage/field.hh>
#include <storage/system_tables.hh>
#include <storage/schema.hh>

#include <sys_tbl_mgr/client.hh>

#include <xid_mgr/xid_mgr_client.hh>

using namespace springtail;

struct PostgresConnection {
    std::string host;
    std::string user;
    std::string password;
    std::string database;
    int port;
};

static constexpr char SCHEMA_TABLES_QUERY[] =
    "SELECT table_name "
    "FROM information_schema.tables "
    "WHERE table_schema = '{}' "
    "AND table_type = 'BASE TABLE' "
    "AND table_schema NOT IN ('pg_catalog', 'information_schema'); ";

void dump_table(const std::filesystem::path &base_dir,
                const std::string &schema_name,
                const std::string &table_name,
                const PostgresConnection &conn,
                uint64_t db_id,
                XidLsn &xid)
{
    SPDLOG_DEBUG("Dumping table {}.{}", schema_name, table_name);

    auto source = std::make_shared<PgCopyTable>(conn.database, schema_name, table_name, "");
    source->connect(conn.host, conn.user, conn.password, conn.port);

    // perform the table copy -- note: updates the LSN of the xid
    source->copy_to_springtail(db_id, xid);
}

void
dump_tables_in_schema(const PostgresConnection &conn,
                      const std::string &schema_name,
                      uint64_t db_id)
{
    std::filesystem::path base_dir;

    // get the base directory for table data
    nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
    Json::get_to<std::filesystem::path>(json, "table_dir", base_dir);
    base_dir = Properties::make_absolute_path(base_dir);

    std::filesystem::create_directories(base_dir);

    LibPqConnection pg_conn{};
    pg_conn.connect(conn.host, conn.database, conn.user,
                    conn.password, conn.port, false);

    pg_conn.exec(fmt::format(SCHEMA_TABLES_QUERY, schema_name));
    if (pg_conn.ntuples() == 0) {
        pg_conn.clear();
        return;
    }

    std::vector<std::string> table_names;
    for (int i = 0; i < pg_conn.ntuples(); i++) {
        auto table_name = pg_conn.get_string(i, 0);
        table_names.push_back(table_name);
    }
    pg_conn.clear();
    pg_conn.disconnect();

    uint64_t xid = XidMgrClient::get_instance()->get_committed_xid(db_id, 0);

    XidLsn target_xid(xid + 1, 0);
    for (const auto &table_name : table_names) {
        SPDLOG_DEBUG("Dumping table {} in schema {}", table_name, schema_name);
        dump_table(base_dir, schema_name, table_name, conn, db_id, target_xid);
    }

    // finalize the system tables
    sys_tbl_mgr::Client::get_instance()->finalize(db_id, target_xid.xid);

    // update the xid mgr
    XidMgrClient::get_instance()->commit_xid(db_id, target_xid.xid, false);
}

int
main(int argc, char *argv[])
{
    std::string schema_name;
    std::string host_name;
    std::string db_name;
    std::string user_name;
    std::string password;
    int port = 5432;
    uint64_t db_id = 1;

    springtail_init();

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("schema,s", boost::program_options::value<std::string>(&schema_name)->required(),
         "Schema name to dump")
        ("host,H", boost::program_options::value<std::string>(&host_name)->default_value("localhost"),
         "Postgres host name")
        ("database,d", boost::program_options::value<std::string>(&db_name)->default_value("postgres"),
         "Postgres database name")
        ("user,u", boost::program_options::value<std::string>(&user_name)->default_value("postgres"),
         "Postgres user name")
        ("password,p", boost::program_options::value<std::string>(&password)->default_value("springtail"),
         "Postgres password")
        ("port,P", boost::program_options::value<int>(&port)->default_value(5432),
         "Postgres port number")
        ("db_id, i", boost::program_options::value<uint64_t>(&db_id)->default_value(1),"Database ID");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    boost::program_options::notify(vm);

    PostgresConnection conn{
        .host = host_name,
        .user = user_name,
        .password = password,
        .database = db_name,
        .port = port
    };

    dump_tables_in_schema(conn, schema_name, db_id);
}
