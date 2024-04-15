#include "common/common.hh"
#include "pg_repl/pg_stream_table.hh"
#include "storage/constants.hh"
#include "storage/field.hh"
#include "storage/io_request.hh"
#include "storage/schema.hh"
#include "storage/schema_type.hh"
#include <boost/algorithm/string.hpp>

namespace springtail
{
    Ingest::Ingest(PgCopyTable &source,
                   std::string &path) {

        springtail_init();

        std::string pg_xids = source->get_xact_xids();
        PgTableSchema pg_schema = source->get_schema()

        //TODO: split this block out into separate function
        std::vector<std::string> xids;
        boost::split(xids, pg_xids, boost::is_any_of(":"));
        //TODO: put start_xid and end_xid somewhere: xids.front(), xids.at(1)

        ExtentSchemaPtr schema = populate_schema(pg_schema.columns);

        ExtentPtr extent = std::make_shared<Extent>(schema, ExtentType{false}, 0);

        populate_rows(schema, extent);

        // TODO make PgMsgTable entry and call create_table
        // springtail/src/storage/table_mgr.cc
        // TableMgr::get_instance()->create_table()
    }

    std::vector<SchemaColumn> Ingest::populate_schema(std::vector<PgColumn> pg_columns) {
        std::vector<SchemaColumn> columns;
        for(PgColumn &pg_col : pg_columns){
            columns.emplace_back(
                0, //internal xid
                pg_col.name, //name
                strToSchemaType(pg_col.type) //SchemaType type
                pg_col.is_nullable //nullable?
                pg_col.default_value //default_value
            );
        }
        return std::make_shared<ExtentSchema>(columns);
    }

    void Ingest::populate_rows(ExtentSchemaPtr schema, ExtentPtr extent, PgStreamTable table) {
        MutableFieldArrayPtr fields = schema->get_mutable_fields()
        MutableFieldArrayPtr values;
        while(values = table.next_row()){
            Extent::Row row = extent->append();
            auto insert_tuple = KeyValueTuple data(schema->get_fields(), values, row);
            // btree->insert(insert_tuple)
            // TODO check extent length, create new if over constant::MAX_EXTENT_SIZE
            // then add row from extent::back()'s' primary key into btree
    }
}
