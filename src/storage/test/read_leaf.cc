#include <common/init.hh>

#include <storage/csv_field.hh>
#include <storage/btree.hh>
#include <storage/mutable_btree.hh>

using namespace springtail;

int
main(int argc,
     char *argv[])
{
    std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
    runners.emplace();
    runners->emplace_back(std::make_unique<IOMgrRunner>());
    springtail_init(runners);

    // construct a schema for testing
    std::vector<SchemaColumn> columns({
            { "table_id", 0, SchemaType::UINT64, 0, false },
            { "name", 1, SchemaType::TEXT, 0, false },
            { "offset", 2, SchemaType::UINT64, 0, false }
        });
    auto schema = std::make_shared<ExtentSchema>(columns);
    auto field = schema->get_field("name");

    auto iomgr = IOMgr::get_instance();

    // construct a mutable b-tree for inserting data
    std::shared_ptr<IOHandle> handle = iomgr->open(argv[1], IOMgr::IO_MODE::READ, true);

    uint64_t extent_id = std::stoull(argv[2]);
    auto response = handle->read(extent_id);
    Extent extent(response->data);

    int count = 0;
    for (auto &&row : extent) {
        std::cout << field->get_text(row) << std::endl;
        ++count;
    }
    std::cout << count << std::endl;
    springtail_shutdown();
}
