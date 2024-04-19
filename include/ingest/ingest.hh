#include "pg_repl/pg_stream_table.hh"
#include "storage/schema.hh"
#include <vector>

namespace springtail
{
    class Ingest {

    Ingest(PgStreamTable &source, std::string &path);

    public:

    ExtentSchemaPtr populate_schema(std::vector<PgColumn> pg_columns);

    private:
    std::vector<PgMsgSchemaColumn> map_to_pg_msg(PgTableSchema);
    void populate_rows(ExtentSchemaPtr schema, PgStreamTable table);

    };
}
