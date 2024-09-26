#include <common/common.hh>
#include <common/constants.hh>

#include <storage/field.hh>
#include <storage/system_tables.hh>
#include <storage/table_mgr.hh>

using namespace springtail;

int
main(int argc,
     char *argv[])
{
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <db_id>" << std::endl;
        return 1;
    }

    springtail_init();

    // takes the database ID from the first argument
    uint64_t db_id = std::stoull(argv[1]);

    // go through each system table and print it out

    for (auto table_id : { sys_tbl::TableNames::ID,
                           sys_tbl::TableRoots::ID,
                           sys_tbl::Indexes::ID,
                           sys_tbl::Schemas::ID,
                           sys_tbl::TableStats::ID }) {
        auto table = TableMgr::get_instance()->get_table(db_id,
                                                         table_id,
                                                         constant::LATEST_XID);
        auto fields = table->extent_schema()->get_fields();

        std::cout << fmt::format("TABLE: {}", table_id) << std::endl;
        for (auto row : (*table)) {
            std::cout << FieldTuple(fields, row).to_string() << std::endl;
        }
    }
}
