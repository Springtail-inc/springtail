#include <filesystem>
#include <map>

#include <fmt/format.h>
#include <boost/program_options.hpp>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <pg_repl/pg_copy_table.hh>
#include <pg_repl/libpq_connection.hh>

#include <storage/table.hh>
#include <storage/table_mgr.hh>
#include <storage/constants.hh>
#include <storage/field.hh>
#include <storage/system_tables.hh>
#include <storage/schema.hh>

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

static constexpr char SERVER_NAME[] = "springtail_fdw_server";
static constexpr char SPRINGTAIL_CATALOG_SCHEMA[] = "__springtail_catalog";

void dump_table(const std::filesystem::path &base_dir,
                const std::string &schema_name,
                const std::string &table_name,
                const PostgresConnection &conn,
                uint64_t xid=2)
{
    SPDLOG_DEBUG("Dumping table {}.{}", schema_name, table_name);

    auto source = std::make_shared<PgCopyTable>(conn.database, schema_name, table_name, "");
    source->connect(conn.host, conn.user, conn.password, conn.port);

    // perform the table copy
    source->copy_to_springtail(base_dir, xid);
}

void
dump_tables_in_schema(const PostgresConnection &conn,
                      const std::string &schema_name)
{
    std::filesystem::path base_dir;

    // get the base directory for table data
    nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
    Json::get_to<std::filesystem::path>(json, "table_dir", base_dir,
                                        "/opt/springtail/table");

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

    uint64_t xid = 2;
    for (const auto &table_name : table_names) {
        SPDLOG_DEBUG("Dumping table {} in schema {}", table_name, schema_name);
        dump_table(base_dir, schema_name, table_name, conn, xid);
        xid += 2;
    }
}

void
gen_fdw_table(const std::string &schema,
              const std::string &table,
              uint64_t tid,
              const std::vector<std::tuple<std::string, uint8_t, bool, std::optional<std::string>>> &columns)
{
    // XXX note: schema, table, column not quoted well...e.g., if they contain double quotes
    std::string create = fmt::format("CREATE FOREIGN TABLE \"{}\".\"{}\" (\n", schema, table);

    // iterate over the columns, adding each to the create statement
    // name, type, is_nullable, default value
    for (int i = 0; i < columns.size(); i++) {
        const auto &[column_name, type, nullable, default_value] = columns[i];
        std::string column = fmt::format("  \"{}\" ", column_name);

        switch (static_cast<SchemaType>(type)) {
            case SchemaType::UINT8: // XXX no good mapping; use smallint for now
            case SchemaType::INT8:
                column += "SMALLINT";
                break;
            case SchemaType::UINT32:
            case SchemaType::INT32:
                column += "INTEGER";
                break;
            case SchemaType::UINT16:
            case SchemaType::INT16:
                column += "SMALLINT";
                break;
            case SchemaType::UINT64:
            case SchemaType::INT64:
                column += "BIGINT";
                break;
            case SchemaType::FLOAT32:
                column += "FLOAT4";
                break;
            case SchemaType::FLOAT64:
                column += "FLOAT8";
                break;
            case SchemaType::BOOLEAN:
                column += "BOOLEAN";
                break;
            case SchemaType::TEXT:
                column += "TEXT";
                break;
            case SchemaType::DATE:
                column += "DATE";
                break;
            case SchemaType::TIME:
                column += "TIME";
                break;
            case SchemaType::TIMESTAMP:
                column += "TIMESTAMP";
                break;
            case SchemaType::BINARY:
                column += "BYTEA";
                break;
            default:
                SPDLOG_ERROR("Unknown type {}", type);
                return;
        }

        // add nullability and default
        if (!nullable) {
            column += " NOT NULL";
        }

        if (default_value.has_value() && !default_value.value().empty()) {
            column += fmt::format(" DEFAULT {}", default_value.value());
        }

        if (i < columns.size() - 1) {
            column += ",\n";
        }

        create += column;
    }

    create += fmt::format("\n) SERVER {} OPTIONS (tid '{}');", SERVER_NAME, tid);

    fmt::print("{}\n", create);
}

void
gen_fdw_system_table(const std::string &table_name,
                     uint64_t tid,
                     const std::vector<SchemaColumn> &schema)
{
    std::string schema_name = SPRINGTAIL_CATALOG_SCHEMA;
    // column description: name, type, nullable, default
    std::vector<std::tuple<std::string, uint8_t, bool, std::optional<std::string>>> columns;

    for (const auto &column : schema) {
        columns.push_back({column.name, (uint8_t)column.type, column.nullable, column.default_value});
    }

    gen_fdw_table(schema_name, table_name, tid, columns);
}

void
gen_fdw_system_tables()
{
    gen_fdw_system_table("table_names", sys_tbl::TableNames::ID, sys_tbl::TableNames::Data::SCHEMA);
    gen_fdw_system_table("table_roots", sys_tbl::TableRoots::ID, sys_tbl::TableRoots::Data::SCHEMA);
    gen_fdw_system_table("indexes", sys_tbl::Indexes::ID, sys_tbl::Indexes::Data::SCHEMA);
    gen_fdw_system_table("schemas", sys_tbl::Schemas::ID, sys_tbl::Schemas::Data::SCHEMA);
}

void
find_primary_idx(auto &idx_iter,
                 auto &idx_table,
                 auto &idx_fields,
                 uint64_t current_tid,
                 uint64_t current_xid,
                 std::vector<uint32_t> &primary_keys)
{
    // search through the index table to find the primary keys for this tid
    // it is sorted in same order as the schemas table
    while (idx_iter != idx_table->end()) {
        auto &idx_row = *idx_iter;

        uint64_t idx_tid = idx_fields->at(sys_tbl::Indexes::Data::TABLE_ID)->get_uint64(idx_row);
        uint64_t idx_id = idx_fields->at(sys_tbl::Indexes::Data::INDEX_ID)->get_uint64(idx_row);
        uint64_t idx_xid = idx_fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(idx_row);

        if (idx_tid == current_tid && idx_id == 0 && idx_xid == current_xid) {
            uint32_t column_id = idx_fields->at(sys_tbl::Indexes::Data::COLUMN_ID)->get_uint32(idx_row);
            primary_keys.push_back(column_id);
        } else if (idx_tid > current_tid) {
            break;
        }

        idx_iter++;
    }
}

void
gen_fdw_schema()
{
    auto table = TableMgr::get_instance()->get_table(sys_tbl::TableNames::ID,
                                                     constant::LATEST_XID,
                                                     constant::MAX_LSN);
    // get field array
    auto fields = table->extent_schema()->get_fields();

    // map from schema -> table name -> <table id, xid>
    std::map<std::string, std::map<std::string, std::pair<uint64_t,uint64_t>>> table_map;

    // iterate over the table names table and populate the table map
    for (auto row : (*table)) {
        std::string schema_name = fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(row);
        std::string table_name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row);
        uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(row);
        uint64_t xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(row);
        bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(row);

        // check if table already exists in the map
        if (exists) {
            SPDLOG_DEBUG("Found table {}.{} tid={}, xid={}\n", schema_name, table_name, tid, xid);
            // if so update the xid if it is newer
            auto entry = table_map[schema_name].insert({table_name, {tid, xid}});
            if (entry.second == false) {
                SPDLOG_DEBUG("Table {} already exists in schema {}\n", table_name, schema_name);
                if (entry.first->second.second < xid) {
                    entry.first->second = {tid, xid};
                }
            }
        }
    }

    // reorganize the table_map to be from tid -> {xid, schema, table}
    std::map<uint64_t, std::tuple<uint64_t, std::string, std::string>> tid_map;
    for (const auto &[schema_name, tables] : table_map) {
        for (const auto &[table_name, table_info] : tables) {
            tid_map[table_info.first] = {table_info.second, schema_name, table_name};
        }
    }

    // Move on to iterating through the schemas table

    // column list: name, type, nullable, default
    std::vector<std::tuple<std::string, uint8_t, bool, std::optional<std::string>>> columns;

    uint64_t current_tid=0;
    std::string current_schema;
    std::string current_table;

    // get the schemas table
    table = TableMgr::get_instance()->get_table(sys_tbl::Schemas::ID,
                                                constant::LATEST_XID,
                                                constant::MAX_LSN);

    auto idx_table = TableMgr::get_instance()->get_table(sys_tbl::Indexes::ID,
                                                         constant::LATEST_XID,
                                                         constant::MAX_LSN);

    Table::Iterator idx_iter = idx_table->begin();
    auto idx_fields = idx_table->extent_schema()->get_fields();

    // iterate through it
    fields = table->extent_schema()->get_fields();
    for (auto row : (*table)) {
        uint64_t tid = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(row);

        // check if we have moved to next tid
        if (tid != current_tid) {

            if (!current_table.empty()) {
                // dump this table
                gen_fdw_table(current_schema, current_table, current_tid, columns);
            }

            // reset state
            columns.clear();
            current_tid = tid;

            // do lookup of new tid in map
            auto it = tid_map.find(tid);
            if (it == tid_map.end()) {
                // not found, not sure why, but skip it
                fmt::print("Table {} not found in schemas table\n", tid);
                continue;
            }

            // update current vars based on this tid and info from tid_map
            current_schema = std::get<1>(it->second);
            current_table = std::get<2>(it->second);
        }

        bool exists = fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(row);
        if (!exists) {
            continue;
        }

        // add column if it exists
        std::string column_name = fields->at(sys_tbl::Schemas::Data::NAME)->get_text(row);
        uint8_t type = fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(row);
        bool nullable = fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(row);
        std::optional<std::string> default_value{};

        if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(row)) {
            default_value = fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(row);
        }

        columns.push_back({column_name, type, nullable, default_value});
    }

    // process last table
    if (columns.size() > 0) {
        // dump this table
        gen_fdw_table(current_schema, current_table, current_tid, columns);
    }
}

int
main(int argc, char *argv[])
{
    bool copy = false;

    springtail_init();

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("copy,c", boost::program_options::bool_switch(&copy)->default_value(false), "Copy tables from Postgres");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }


    PostgresConnection conn{
        .host = "localhost",
        .user = "postgres",
        .password = "springtail",
        .database = "postgres",
        .port = 5432
    };

    if (copy) {
        dump_tables_in_schema(conn, "public");
    }

    gen_fdw_schema();
    gen_fdw_system_tables();
}
