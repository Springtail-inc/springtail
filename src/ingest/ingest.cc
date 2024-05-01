#include <common/common.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_stream_table.hh>
#include <storage/constants.hh>
#include <storage/field.hh>
#include <storage/io_mgr.hh>
#include <storage/mutable_btree.hh>
#include <storage/schema.hh>
#include <storage/table_mgr.hh>
#include <boost/algorithm/string.hpp>
#include <ingest/ingest.hh>

namespace springtail
{
    Ingest::Ingest(std::shared_ptr<PgStreamTable> source, const std::filesystem::path path) {
        source->get_table_oid();
        std::string pg_xids = source->get_xact_xids();
        PgTableSchema pg_schema = source->get_schema();

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
        ExtentSchemaPtr schema = _populate_schema(pg_schema.columns);

        // set up pkey btree
        std::filesystem::path index_path = path;
        index_path /= "0.idx";
        auto btree = std::make_shared<MutableBTree>(index_path, pg_schema.pkeys, write_cache, schema);
        btree->init_empty();

        _populate_rows(io_handle, btree, schema, source, pg_schema.pkeys);

        // make PgMsgTable entry and call create_table
        TableMgr::get_instance()->create_table(pg_schema.table_oid, 0, PgMsgTable{
            0, //lsn
            pg_schema.table_oid,
            0, //xid
            pg_schema.schema_name,
            pg_schema.table_name,
            _map_to_pg_msg(pg_schema.columns, pg_schema.pkeys)
        });
    }

    std::vector<PgMsgSchemaColumn> Ingest::_map_to_pg_msg(const std::vector<PgColumn> pg_columns,
                                                          const std::vector<std::string> pkeys) {
        std::vector<PgMsgSchemaColumn> columns;
        columns.reserve(pg_columns.size());
        for(const PgColumn &pg_col : pg_columns){
            columns.push_back(
                PgMsgSchemaColumn{
                    pg_col.name,
                    pg_col.type,
                    pg_col.default_value,
                    pg_col.position,
                    pg_col.is_pkey ? _get_vec_pos(pkeys, pg_col.name) : -1, // pk_position
                    pg_col.is_nullable,
                    pg_col.is_pkey,
                    // TODO: we assume false since we don't support generated fields rn
                    false  // is_generated
                }
            );
        }
        return columns;
    }

    int Ingest::_get_vec_pos(const std::vector<std::string> vec, const std::string element) {
        auto it = std::find(vec.begin(), vec.end(), element);
        if (it == vec.end())
        {
            return -1;
        } else
        {
            return std::distance(vec.begin(), it);
        }
    }

    ExtentSchemaPtr Ingest::_populate_schema(const std::vector<PgColumn> pg_columns) {
        std::vector<SchemaColumn> columns;
        auto tbl_mgr = TableMgr::get_instance();
        for(const PgColumn &pg_col : pg_columns){
            columns.push_back(
                SchemaColumn{
                    0, //internal xid
                    0, //lsn
                    pg_col.name, //name
                    static_cast<uint32_t>(pg_col.position),
                    tbl_mgr->convert_pg_type(pg_col.type), //SchemaType type
                    true, //exists?
                    pg_col.is_nullable, //nullable?
                    pg_col.default_value //default_value
                }
            );
        }
        return std::make_shared<ExtentSchema>(columns);
    }

    void Ingest::_populate_rows(const std::shared_ptr<IOHandle> io_handle,
                                const std::shared_ptr<MutableBTree> btree,
                                const ExtentSchemaPtr schema,
                                const std::shared_ptr<PgStreamTable> table,
                                const std::vector<std::string> pkeys) {
        auto extent = std::make_shared<Extent>(schema, ExtentType{false}, 0);

        // initiate state on pg connection for data copying
        table->copy_data();

        std::optional<FieldArrayPtr> values;
        std::shared_ptr<MutableTuple> insert_tuple;
        MutableFieldArrayPtr fields = schema->get_mutable_fields();
        while((values = table->next_row())){
            Extent::Row row = extent->append();
            insert_tuple = std::make_shared<MutableTuple>(fields, row);
            insert_tuple->assign(FieldTuple(values.value(), nullptr));
            // write the new extent to disk
            auto &&future = extent->async_flush(io_handle);

            // construct a new root page based on the new extent
            uint64_t extent_id = future.get()->offset;
            //if the next row will go over the max extent size, then flush and make a new one
            if(extent->byte_count() + extent->row_size() >= constant::MAX_EXTENT_SIZE){
                //grab pkey fields from the last insert, add `extent_id` to end of the fieldarray
                FieldArrayPtr fields = schema->fieldarray_subset(insert_tuple, pkeys);
                fields->push_back(std::make_shared<ConstTypeField<uint64_t>>(extent_id));
                btree->insert(std::make_shared<FieldTuple>(fields, nullptr));
                
                //create new extent
                extent = std::make_shared<Extent>(schema, ExtentType{false}, 0);
            }
        }
    }
}
