#include <common/common.hh>

#include <storage/constants.hh>
#include <storage/field.hh>
#include <storage/system_tables.hh>
#include <storage/table_mgr.hh>

using namespace springtail;

int
main(int argc,
     char *argv[])
{
    springtail_init();

    // go through each system table and print it out

    for (auto table_id : { sys_tbl::TableNames::ID,
                           sys_tbl::TableRoots::ID,
                           sys_tbl::Indexes::ID,
                           sys_tbl::Schemas::ID }) {
        auto table = TableMgr::get_instance()->get_table(table_id,
                                                         constant::LATEST_XID,
                                                         constant::MAX_LSN);
        auto fields = table->extent_schema()->get_fields();

        std::cout << fmt::format("TABLE: {}", table_id) << std::endl;
        for (auto row : (*table)) {
            std::cout << FieldTuple(fields, row).to_string() << std::endl;
        }
    }
}
