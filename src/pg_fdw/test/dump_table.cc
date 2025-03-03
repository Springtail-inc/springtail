#include <iostream>

#include <stdint.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdbool.h>

#include <fmt/format.h>
#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>

#include <common/init.hh>
#include <common/constants.hh>
#include <common/json.hh>

#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_fdw_mgr.hh>

#include <storage/field.hh>
#include <storage/schema.hh>

#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/table_mgr.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

/** List all tables from TableNames system table */
void
list_tables(uint64_t db_id)
{
    auto table = TableMgr::get_instance()->get_table(db_id,
                                                     sys_tbl::TableNames::ID,
                                                     constant::LATEST_XID);
    // get field array
    auto fields = table->extent_schema()->get_fields();

    // iterate over the table names table
    for (auto row : (*table)) {
        uint64_t namespace_id = fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(row);
        std::string table_name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row));
        uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(row);
        uint64_t xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(row);
        bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(row);

        // check if table already exists in the map
        if (exists) {
            std::cout << fmt::format("Found table {} ns_id={}, tid={}, xid={}\n", table_name, namespace_id, tid, xid);
        }
    }
}

uint64_t
find_namespace_id(uint64_t db_id, const std::string &name, const XidLsn &xid)
{
    auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::NamespaceNames::ID,
                                                     constant::LATEST_XID);
    auto schema = table->extent_schema();
    auto fields = schema->get_fields();

    auto search_key = sys_tbl::NamespaceNames::Secondary::key_tuple(name, xid.xid, xid.lsn);

    // find the row that matches the name at the given XID/LSN
    auto row_i = table->inverse_lower_bound(search_key, 1);

    // make sure table ID exists at this XID/LSN
    auto name_f = fields->at(sys_tbl::NamespaceNames::Data::NAME);
    if (row_i == table->end() || name_f->get_text(*row_i) != name) {
        SPDLOG_ERROR("No namespace name '{}' at xid {}:{}", name, xid.xid, xid.lsn);
        throw FdwError(fmt::format("No namespace name '{}' at xid {}:{}", name, xid.xid, xid.lsn));
    }

    // make sure that the table is marked as existing at this XID/LSN
    bool exists = fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(*row_i);
    if (!exists) {
        SPDLOG_WARN("Namespace marked non-existant at xid {}:{}", xid.xid, xid.lsn);
        throw FdwError(fmt::format("Namespace '{}' marked non-existant at xid {}:{}",
                                   name, xid.xid, xid.lsn));
    }

    return fields->at(sys_tbl::NamespaceNames::Data::NAMESPACE_ID)->get_uint64(*row_i);
}

/** Lookup table ID from TableNames system table */
uint64_t
lookup_table(uint64_t db_id,
             const std::string &schema_name,
             const std::string &table_name,
             uint64_t xid)
{
    // find the schema name in the namespace names table
    uint64_t namespace_id = find_namespace_id(db_id, schema_name, XidLsn(xid));

    // get the table names table
    auto table = TableMgr::get_instance()->get_table(db_id,
                                                     sys_tbl::TableNames::ID,
                                                     constant::LATEST_XID);
    // get field array
    auto fields = table->extent_schema()->get_fields();

    // iterate over the table names table
    uint64_t found_tid = -1;
    for (auto row : (*table)) {
        uint64_t ns_id = fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(row);
        std::string name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row));
        uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(row);
        uint64_t t_xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(row);

        if (namespace_id == ns_id && table_name == name && t_xid <= xid) {
            found_tid = tid;
        }
    }

    return found_tid;
}

/** Convert datum to string based on schema type */
std::string
dump_datum(Datum value, SchemaType type)
{
    switch (type) {
        case SchemaType::BOOLEAN:
            return DatumGetBool(value) ? "TRUE" : "FALSE";
        case SchemaType::UINT8:
        case SchemaType::INT8:
            return fmt::format("{}", DatumGetChar(value));
        case SchemaType::INT16:
            return fmt::format("{}", DatumGetInt16(value));
        case SchemaType::INT32:
            return fmt::format("{}", DatumGetInt32(value));
        case SchemaType::INT64:
            return fmt::format("{}", DatumGetInt64(value));
        case SchemaType::UINT16:
            return fmt::format("{}", DatumGetUInt16(value));
        case SchemaType::UINT32:
            return fmt::format("{}", DatumGetUInt32(value));
        case SchemaType::UINT64:
            return fmt::format("{}", DatumGetUInt64(value));
        case SchemaType::FLOAT32:
            return fmt::format("{}", DatumGetFloat4(value));
        case SchemaType::FLOAT64:
            return fmt::format("{}", DatumGetFloat8(value));
        case SchemaType::TEXT:
            //return fmt::format("{}", DatumGetCString(value));
            return fmt::format("{}", TextDatumGetCString(value));
        default:
            return "UNKNOWN";
    }
}

/** Dump a table by table ID and xid */
void
dump_table(uint64_t db_id,
           uint64_t tid,
           uint64_t xid)
{
    TablePtr table = TableMgr::get_instance()->get_table(db_id, tid, xid);
    ExtentSchemaPtr schema = table->extent_schema();
    std::map<uint32_t, SchemaColumn> columns = SchemaMgr::get_instance()->get_columns(db_id, tid, { xid, constant::MAX_LSN });

    auto fields = schema->get_fields();
    FormData_pg_attribute attrdata[fields->size()];
    Form_pg_attribute attrs[fields->size()];

    List *target_list = NIL;

    // iterate over column map and extract attrnums
    int i = 0;
    for (auto &col : columns) {
        if (col.second.exists) {
            attrs[i] = &attrdata[i];
            attrs[i]->attnum = col.first;
            attrs[i]->atttypid = col.second.pg_type;
            attrs[i]->atttypmod = -1;

            // append col.first to target_list
            target_list = lappend(target_list, makeInteger(col.first));

            i++;
        }
    }

    PgFdwMgr::fdw_init(nullptr, false);
    PgFdwMgr *mgr = PgFdwMgr::get_instance();

    // create the fdw state for the table @xid and begin the scan
    PgFdwState *state = mgr->fdw_create_state(db_id, tid, xid, xid);
    mgr->fdw_begin_scan(state, target_list, nullptr, nullptr);

    // iterate through the table and print the values
    Datum values[fields->size()];
    bool nulls[fields->size()];
    bool eos = false;
    while (!eos) {
        bool row_valid = mgr->fdw_iterate_scan(state, fields->size(), attrs, values, nulls, &eos);
        if (!row_valid) {
            continue;
        }
        // print the values
        for (size_t j = 0; j < fields->size(); j++) {
            if (nulls[j]) {
                std::cout << "NULL";
            } else {
                std::cout << dump_datum(values[j], fields->at(j)->get_type());
            }
            std::cout << " ";
        }
        std::cout << std::endl;
    }

    // end the scan releasing the state
    mgr->fdw_end_scan(state);
}

/** Dump a table by schema, table name and xid */
void
dump_table(uint64_t db_id,
           const std::string &schema_name,
           const std::string &table_name,
           uint64_t xid)
{
    uint64_t tid = lookup_table(db_id, schema_name, table_name, xid);
    if (tid == -1) {
        std::cerr << fmt::format("Table {}.{} not found\n", schema_name, table_name);
        return;
    }

    dump_table(db_id, tid, xid);
}

int main(int argc, char *argv[])
{

    std::string table;
    std::string schema;
    uint64_t db_id = 0;
    uint64_t xid=0;
    uint64_t tid=0;
    bool list = false;

    springtail_init(std::nullopt, false);

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("db,d", po::value<uint64_t>(&db_id), "Database ID");
    desc.add_options()("table,t", po::value<std::string>(&table), "Table name to dump");
    desc.add_options()("schema,s", po::value<std::string>(&schema)->default_value("public"), "Schema name");
    desc.add_options()("xid,x", po::value<uint64_t>(&xid), "XID");
    desc.add_options()("tid,i", po::value<uint64_t>(&tid), "Table ID");
    desc.add_options()("list,l", po::bool_switch(&list)->default_value(false), "List tables");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (list) {
        std::cout << "Listing tables:" << std::endl;
        list_tables(db_id);
    } else if (!table.empty()) {
        if (xid == 0) {
            std::cerr << "XID must be specified with table name" << std::endl;
            return -1;
        }
        std::cout << "Dump table: " << table << " XID: " << xid << std::endl;
        dump_table(db_id, schema, table, xid);
    } else if (tid != 0) {
        if (xid == 0) {
            std::cerr << "XID must be specified with table ID" << std::endl;
            return -1;
        }
        std::cout << "Dump table: " << tid << " XID: " << xid << std::endl;
        dump_table(db_id, tid, xid);
    } else {
        std::cerr << "List flag, Table name or ID must be specified" << std::endl;
        return -1;
    }

    springtail_shutdown();
    return 0;
}
