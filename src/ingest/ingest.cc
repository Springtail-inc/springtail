#include "common/common.hh"
#include "pg_repl/pg_repl_msg.hh"
#include "pg_repl/pg_stream_table.hh"
#include "storage/constants.hh"
#include "storage/field.hh"
#include "storage/schema.hh"
#include "storage/table_mgr.hh"
#include <boost/algorithm/string.hpp>
#include <ingest/ingest.hh>

namespace springtail
{
    Ingest::Ingest(PgStreamTable &source, std::string &path) {

        springtail_init();

        std::string pg_xids = source.get_xact_xids();
        PgTableSchema pg_schema = source.get_schema();

        std::vector<std::string> xids;
        boost::split(xids, pg_xids, boost::is_any_of(":"));
        //TODO: put start_xid and end_xid somewhere: xids.front(), xids.at(1)

        ExtentSchemaPtr schema = populate_schema(pg_schema.columns);

        populate_rows(schema, source);

        // make PgMsgTable entry and call create_table
        TableMgr::get_instance()->create_table(pg_schema.table_oid, 0, PgMsgTable{
            0, //lsn
            pg_schema.table_oid,
            0, //xid
            pg_schema.schema_name,
            pg_schema.table_name,
            map_to_pg_msg(pg_schema)
        });
    }

    std::vector<PgMsgSchemaColumn> Ingest::map_to_pg_msg(PgTableSchema schema){
        
    }

    ExtentSchemaPtr Ingest::populate_schema(std::vector<PgColumn> pg_columns) {
        std::vector<SchemaColumn> columns;
        for(PgColumn &pg_col : pg_columns){
            columns.emplace_back(
                SchemaColumn(
                    0, //internal xid
                    0, //lsn
                    pg_col.name, //name
                    pg_col.position,
                    strToSchemaType(pg_col.type), //SchemaType type
                    true, //exists?
                    pg_col.is_nullable, //nullable?
                    pg_col.default_value //default_value
                )
            );
        }
        return std::make_shared<ExtentSchema>(columns);
    }

    void Ingest::populate_rows(ExtentSchemaPtr schema, PgStreamTable table) {
        auto extent = std::make_shared<Extent>(schema, ExtentType{false}, 0);

        table.copy_data();
        std::optional<FieldArrayPtr> values;
        while((values = table.next_row())){
            if(extent->byte_count() + extent->row_size() >= constant::MAX_EXTENT_SIZE){
                // XXX add row from extent::back()'s primary key into btree
                // btree->insert(insert_tuple)
                extent.reset(new Extent(schema, ExtentType{false}, 0));
            }
            Extent::Row row = extent->append();
            auto insert_tuple = KeyValueTuple(schema->get_fields(), values.value(), row);
        }
    }
}
