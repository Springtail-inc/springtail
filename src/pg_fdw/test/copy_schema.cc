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

    PgCopyResultPtr res = PgCopyTable::copy_schema(db_id, schema_name);

    // update the xid mgr
    XidMgrClient::get_instance()->commit_xid(db_id, res->target_xid, false);
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
