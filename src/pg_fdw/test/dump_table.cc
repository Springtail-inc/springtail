#include <iostream>

#include <stdint.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdbool.h>

#include <fmt/format.h>
#include <boost/program_options.hpp>

#include <common/common.hh>
#include <common/constants.hh>
#include <pg_fdw/pg_fdw_mgr.hh>

#include <storage/table.hh>
#include <storage/table_mgr.hh>
#include <storage/field.hh>
#include <storage/system_tables.hh>
#include <storage/schema.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

/** List all tables from TableNames system table */
void
list_tables()
{
    auto table = TableMgr::get_instance()->get_table(sys_tbl::TableNames::ID,
                                                     constant::LATEST_XID,
                                                     constant::MAX_LSN);
    // get field array
    auto fields = table->extent_schema()->get_fields();

    // iterate over the table names table
    for (auto row : (*table)) {
        std::string schema_name(fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(row));
        std::string table_name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row));
        uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(row);
        uint64_t xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(row);
        bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(row);

        // check if table already exists in the map
        if (exists) {
            std::cout << fmt::format("Found table {}.{} tid={}, xid={}\n", schema_name, table_name, tid, xid);
        }
    }
}

/** Lookup table ID from TableNames system table */
uint64_t
lookup_table(const std::string &schema_name, const std::string &table_name, uint64_t xid)
{
    // get the table names table
    auto table = TableMgr::get_instance()->get_table(sys_tbl::TableNames::ID,
                                                     constant::LATEST_XID,
                                                     constant::MAX_LSN);
    // get field array
    auto fields = table->extent_schema()->get_fields();

    // iterate over the table names table
    uint64_t found_tid = -1;
    for (auto row : (*table)) {
        std::string ns(fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(row));
        std::string name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row));
        uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(row);
        uint64_t t_xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(row);

        if (schema_name == ns && table_name == name && t_xid <= xid) {
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
dump_table(uint64_t tid, uint64_t xid)
{
    TablePtr table = TableMgr::get_instance()->get_table(tid, xid, constant::MAX_LSN);
    ExtentSchemaPtr schema = table->extent_schema();
    std::map<uint32_t, SchemaColumn> columns = SchemaMgr::get_instance()->get_columns(tid, { xid, constant::MAX_LSN });

    auto fields = schema->get_fields();
    FormData_pg_attribute attrdata[fields->size()];
    Form_pg_attribute attrs[fields->size()];

    // iterate over column map and extract attrnums
    int i = 0;
    for (auto &col : columns) {
        if (col.second.exists) {
            attrs[i] = &attrdata[i];
            attrs[i]->attnum = col.first;
            attrs[i]->atttypid = col.second.pg_type;
            attrs[i]->atttypmod = -1;
        }
    }

    PgFdwMgr *mgr = PgFdwMgr::get_instance();

    // create the fdw state for the table @xid and begin the scan
    PgFdwState *state = mgr->fdw_create_state(tid, xid);
    mgr->fdw_begin_scan(state, nullptr, nullptr, nullptr);

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
dump_table(const std::string &schema_name, const std::string &table_name, uint64_t xid)
{
    uint64_t tid = lookup_table(schema_name, table_name, xid);
    if (tid == -1) {
        std::cerr << fmt::format("Table {}.{} not found\n", schema_name, table_name);
        return;
    }

    dump_table(tid, xid);
}

int main(int argc, char *argv[])
{
    springtail_init(0);

    std::string table;
    std::string schema;
    uint64_t xid=0;
    uint64_t tid=0;
    bool list = false;

    springtail::springtail_init();

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("table,t", boost::program_options::value<std::string>(&table), "Table name to dump")
        ("schema,s", boost::program_options::value<std::string>(&schema)->default_value("public"), "Schema name")
        ("xid,x", boost::program_options::value<uint64_t>(&xid), "XID")
        ("tid,i", boost::program_options::value<uint64_t>(&tid), "Table ID")
        ("list,l", boost::program_options::bool_switch(&list)->default_value(false), "List tables");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (list) {
        std::cout << "Listing tables:" << std::endl;
        list_tables();
    } else if (!table.empty()) {
        if (xid == 0) {
            std::cerr << "XID must be specified with table name" << std::endl;
            return -1;
        }
        std::cout << "Dump table: " << table << " XID: " << xid << std::endl;
        dump_table(schema, table, xid);
    } else if (tid != 0) {
        if (xid == 0) {
            std::cerr << "XID must be specified with table ID" << std::endl;
            return -1;
        }
        std::cout << "Dump table: " << tid << " XID: " << xid << std::endl;
        dump_table(tid, xid);
    } else {
        std::cerr << "List flag, Table name or ID must be specified" << std::endl;
        return -1;
    }

    return 0;
}
