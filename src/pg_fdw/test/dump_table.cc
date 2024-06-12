#include <iostream>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <common/common.hh>

#include <pg_fdw/pg_fdw_mgr.hh>

#include <storage/table.hh>
#include <storage/table_mgr.hh>
#include <storage/constants.hh>
#include <storage/field.hh>
#include <storage/system_tables.hh>
#include <storage/schema.hh>

using namespace springtail;

extern "C" {
    /** Dummy function so that we can link with pg_fdw_mgr.cc */
    text *
    cstring_to_text(const char *s)
    {
        int len = strlen(s);
        text *result = (text *) palloc(VARHDRSZ + len + 1);
        return result;
    }
}

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
        std::string schema_name = fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(row);
        std::string table_name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row);
        uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(row);
        uint64_t xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(row);
        bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(row);

        // check if table already exists in the map
        if (exists) {
            std::cout << fmt::format("Found table {}.{} tid={}, xid={}\n", schema_name, table_name, tid, xid);
        }
    }
}

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
        std::string ns = fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(row);
        std::string name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row);
        uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(row);
        uint64_t t_xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(row);

        if (schema_name == ns && table_name == name && t_xid <= xid) {
            found_tid = tid;
        }
    }

    return found_tid;
}

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
            return fmt::format("{}", DatumGetCString(value));
        case SchemaType::TIMESTAMP:
            return fmt::format("{}", DatumGetUInt64(value));
        case SchemaType::DATE:
            return fmt::format("{}", DatumGetUInt32(value));
        case SchemaType::TIME:
            return fmt::format("{}", DatumGetUInt64(value));
        default:
            return "UNKNOWN";
    }
}

void
dump_table(uint64_t tid, uint64_t xid)
{
    TablePtr table = TableMgr::get_instance()->get_table(tid, xid, constant::MAX_LSN);
    ExtentSchemaPtr schema = table->extent_schema();
    auto fields = schema->get_fields();

    PgFdwMgr *mgr = PgFdwMgr::get_instance();

    PgFdwState *state = mgr->fdw_begin(tid, xid);

    Datum values[fields->size()];
    bool nulls[fields->size()];
    while (mgr->fdw_iterate_scan(state, values, nulls)) {
        // print the values
        for (size_t i = 0; i < fields->size(); i++) {
            if (nulls[i]) {
                std::cout << "NULL";
            } else {
                std::cout << dump_datum(values[i], fields->at(i)->get_type());
            }
            std::cout << " ";
        }
        std::cout << std::endl;
    }

    mgr->fdw_end(state);
}

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
    }

    return 0;
}
