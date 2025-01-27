#include <common/common.hh>
#include <common/constants.hh>
#include <storage/field.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table_mgr.hh>

using namespace springtail;

int
main(int argc, char *argv[])
{
    springtail_init();

    // open the file and scan the header of each extent
    auto handle = IOMgr::get_instance()->open(argv[1], IOMgr::IO_MODE::READ, true);

    uint64_t pos = 0;
    uint64_t max = std::filesystem::file_size(argv[1]);
    while (pos < max) {
        auto response = handle->read(pos);

        Extent e(response->data);
        std::cout << fmt::format("Extent @ {}", pos) << std::endl
                  << fmt::format("\ttype: {}, xid: {}, row_size: {}, prev: {}",
                                 static_cast<uint8_t>(e.header().type), e.header().xid,
                                 e.header().row_size, e.header().prev_offset)
                  << std::endl;

        pos += response->next_offset;
    }
}
