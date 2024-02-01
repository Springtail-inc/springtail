#include <storage/btree.hh>

int
main(int argc,
     char *argv[])
{
    using namespace springtail;

    std::shared_ptr<ExtentSchema> schema =
        std::make_shared<ExtentSchema>(std::vector<SchemaColumn>({
                    { "table_id", 0, SchemaType::UINT64, false },
                    { "name", 1, SchemaType::TEXT, false },
                    { "offset", 2, SchemaType::UINT64, false }
                }));

    std::shared_ptr<IOHandle> handle = IOMgr::get_instance()->open("/tmp/test.bt", IOMgr::IO_MODE::WRITE, true);
    std::vector<std::string> keys({"table_id", "name"});
    uint64_t cache_size = 1024*1024*1024;
    uint64_t extent_id = 0;
    uint64_t xid = 0;

    MutableBTree tree(handle, keys, extent_id, cache_size, schema, xid);

    return 0;
}
