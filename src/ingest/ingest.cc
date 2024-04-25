#include "common/common.hh"
#include "pg_repl/pg_repl_msg.hh"
#include "pg_repl/pg_stream_table.hh"
#include "storage/constants.hh"
#include "storage/field.hh"
#include "storage/io_mgr.hh"
#include "storage/mutable_btree.hh"
#include "storage/schema.hh"
#include "storage/table_mgr.hh"
#include <boost/algorithm/string.hpp>
#include <ingest/ingest.hh>

namespace springtail
{
    Ingest::Ingest(PgStreamTable &source, std::filesystem::path path) {

        springtail_init();

        std::string pg_xids = source.get_xact_xids();
        PgTableSchema pg_schema = source.get_schema();

        std::vector<std::string> xids;
        boost::split(xids, pg_xids, boost::is_any_of(":"));
        //TODO: put start_xid and end_xid somewhere: xids.front(), xids.at(1)

        // set up io_handle path for extents
        std::filesystem::path extent_path = path;
        extent_path /= "raw";
        std::shared_ptr<IOHandle> io_handle = IOMgr::get_instance()->open(extent_path, IOMgr::IO_MODE::APPEND, true);
        
        // set up write cache
        std::shared_ptr<MutableBTree::PageCache> write_cache = MutableBTree::create_cache(2*1024*1024);

        // generate schema
        ExtentSchemaPtr schema = populate_schema(pg_schema.columns);

        // set up pkey btree
        std::filesystem::path index_path = path;
        index_path /= "0.idx";
        auto btree = std::make_shared<MutableBTree>(index_path, pg_schema.pkeys, write_cache, schema);
        btree->init_empty();

        populate_rows(io_handle, btree, schema, source, pg_schema.pkeys);

        // make PgMsgTable entry and call create_table
        TableMgr::get_instance()->create_table(pg_schema.table_oid, 0, PgMsgTable{
            0, //lsn
            pg_schema.table_oid,
            0, //xid
            pg_schema.schema_name,
            pg_schema.table_name,
            map_to_pg_msg(pg_schema.columns, pg_schema.pkeys)
        });
    }

    std::vector<PgMsgSchemaColumn> Ingest::map_to_pg_msg(std::vector<PgColumn> pg_columns, std::vector<std::string> pkeys) {
        std::vector<PgMsgSchemaColumn> columns(pg_columns.size());
        for(PgColumn &pg_col : pg_columns){
            columns.emplace_back(
                PgMsgSchemaColumn(
                    pg_col.name,
                    pg_col.type,
                    pg_col.default_value,
                    pg_col.position,
                    pg_col.is_pkey ? get_vec_pos(pkeys, pg_col.name) : -1, // pk_position
                    pg_col.is_nullable,
                    pg_col.is_pkey,
                    // TODO: we assume false since we don't support generated fields rn
                    false  // is_generated
                )
            );
        }
        return columns;
    }

    int Ingest::get_vec_pos(std::vector<std::string> vec, std::string element) {
        auto it = std::find(vec.begin(), vec.end(), element);
        if (it == vec.end())
        {
            return -1;
        } else
        {
            return std::distance(vec.begin(), it);
        }
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

    void Ingest::populate_rows(std::shared_ptr<IOHandle> io_handle, std::shared_ptr<MutableBTree> btree,
                               ExtentSchemaPtr schema, PgStreamTable table, std::vector<std::string> pkeys) {
        auto extent = std::make_shared<Extent>(schema, ExtentType{false}, 0);

        // initiate state on pg connection for data copying
        table.copy_data();

        std::optional<FieldArrayPtr> values;
        std::shared_ptr<MutableTuple> insert_tuple;
        MutableFieldArrayPtr fields = schema->get_mutable_fields();
        while((values = table.next_row())){
            Extent::Row row = extent->append();
            insert_tuple.reset(new MutableTuple(fields, row));
            insert_tuple->assign(FieldTuple(values.value(), nullptr));
            // write the new extent to disk
            auto &&future = extent->async_flush(io_handle);

            // construct a new root page based on the new extent
            uint64_t extent_id = future.get()->offset;
            //if the next row will go over the max extent size, then flush and make a new one
            if(extent->byte_count() + extent->row_size() >= constant::MAX_EXTENT_SIZE){
                //grab pkey fields from the last insert, add `extent_id` to end of the fieldarray
                FieldArrayPtr fields = schema->fieldarray_subset(insert_tuple, pkeys);
                fields->push_back(std::make_shared<ConstTypeField<std::string>>(extent_id));
                btree->insert(std::make_shared<FieldTuple>(fields, nullptr));
                
                //create new extent
                extent.reset(new Extent(schema, ExtentType{false}, 0));
            }
        }
    }
}
