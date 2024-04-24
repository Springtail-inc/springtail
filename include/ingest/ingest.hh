#include "pg_repl/pg_stream_table.hh"
#include "storage/mutable_btree.hh"
#include "storage/schema.hh"
#include <vector>

namespace springtail
{
    class Ingest {

    Ingest(PgStreamTable &source, std::filesystem::path path);

    public:

    ExtentSchemaPtr populate_schema(std::vector<PgColumn> pg_columns);

    private:
    std::vector<PgMsgSchemaColumn> map_to_pg_msg(std::vector<PgColumn>, std::vector<std::string> pkeys);
    void populate_rows(std::shared_ptr<IOHandle> io_handle, std::shared_ptr<MutableBTree> btree, ExtentSchemaPtr schema, PgStreamTable table, std::vector<std::string> pkeys);
    int get_vec_pos(std::vector<std::string> vec, std::string element);

    };
}
