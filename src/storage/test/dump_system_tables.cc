#include <common/init.hh>
#include <common/constants.hh>

#include <storage/field.hh>
#include <storage/io_mgr.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/system_table_mgr_server.hh>

using namespace springtail;

int
main(int argc,
     char *argv[])
{
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <db_id>" << std::endl;
        return 1;
    }

    // no logging
    springtail_init(false, std::nullopt, LOG_NONE);

    // takes the database ID from the first argument
    uint64_t db_id = std::stoull(argv[1]);

    // go through each system table and print it out

    for (auto table_id : { sys_tbl::TableNames::ID,
                           sys_tbl::TableRoots::ID,
                           sys_tbl::Indexes::ID,
                           sys_tbl::Schemas::ID,
                           sys_tbl::TableStats::ID,
                           sys_tbl::IndexNames::ID,
                           sys_tbl::NamespaceNames::ID,
                           sys_tbl::UserTypes::ID }) {
        auto table = SystemTableMgrServer::get_instance()->get_table(db_id,
                                                                  table_id,
                                                                  constant::LATEST_XID);
        auto schema = table->extent_schema();
        auto fields = schema->get_fields();

        std::cout << fmt::format("\nTABLE: {} ({})\n", sys_tbl::SYSTEM_TABLE_NAMES.at(table_id), table_id);

        for (const auto &name : schema->column_order()) {
            std::cout << name << ":";
        }
        std::cout << std::endl;

        for (auto row : (*table)) {
            std::cout << FieldTuple(fields, &row).to_string() << std::endl;
        }
    }
    springtail_shutdown();
}
