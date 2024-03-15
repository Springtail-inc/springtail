#include <storage/table_manager.hh>

namespace springtail {
    TablePtr
    TableManager::get_table(uint64_t table_id, uint64_t xid, uint64_t lsn)
    {
        boost::scoped_lock lock(_mutex);

        // check the system tables
        // XXX how to populate the system tables?
        auto table_i = _system_tables.find(table_id);
        if (table_i != _system_tables.end()) {
            return table_i->second;
        }

        // check the table cache
        table_i = _table_cache.find(table_id);
        if (table_i != _table_cache.end()) {
            return table_i->second;
        }

        // read the table metadata from the appropriate system table
        auto tables_t = _system_tables.find(TABLES_TABLE)->second;

        auto key_fields = std::make_shared<FieldArray>(3);
        key_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
        key_fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(xid);
        key_fields->at(2) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
        auto search_key = std::make_shared<FieldTuple>(key_fields);

        auto pos_i = tables_t->primary_lower_bound(search_key);

        auto schema = tables_t->get_schema(xid, lsn);
        auto fields = schema->get_fields("table_id", "xid", "lsn");

        // if the lower_bound() found a greater entry, we need to go back one to find the row that
        // covers this XID/LSN
        if (search_key->less_than(FieldTuple(fields, *pos_i))) {
            --pos_i;
        }

        if (pos_i == tables_t->primary->end()) {
            return nullptr;
        }

        if (fields->at(0)->get_uint64(*pos_i) != table_id) {
            return nullptr;
        }

        auto table = std::make_shared<Table>(table_id,
                                             _cache,
                                             fields->at(XID)->get_uint64(*pos_i),
                                             fields->at(EXTENT_ID)->get_uint64(*pos_i));
        return table;
    }

    void
    TableManager::create_table(uint64_t xid,
                               uint64_t lsn,
                               const PgMsgTable &msg)
    {
        // get "tables" system table
        auto tables_t = get_table(TABLES_TABLE);

        // add a table-create entry that starts the table at the given XID/LSN
        tuple = msg.oid, msg.schema, msg.table, xid, lsn, true;
        tables_t->insert(tuple);

        // 2) add a row to "schemas" for each column
        for (auto &&column : msg.columns) {
            // XXX need to map the udt_type
            SchemaColumn scolumn(xid, lsn, column.column_name, column.position, column.udt_type, column.is_nullable, column.default_value);
            SchemaUpdate update(xid, lsn, SchemaUpdateType::ADD_COLUMN, column.position, column.column_name, column.udt_type, column.is_nullable, column.default_value);

            _schema_manager->alter_schema(msg.oid, scolumn, update);
        }

        // 3) if there's a primary key, add a row to "primary_indexes" for each key column
        // XXX need the primary key columns and order
    }

    void
    TableManager::alter_table(uint64_t xid,
                              uint64_t lsn,
                              const PgMsgTable &msg)
    {
        // get "tables" system table
        auto tables_t = get_table(TABLES_TABLE);

        // XXX check what the name of the table is currently by looking up the msg.oid
        auto row = tables_t->secondary_index.find(msg.oid, xid, lsn);

        if (namespace_f->get_text(row) != msg.schema ||
            name_f->get_text(row) != msg.table) {
            // 1) if the table name is changed, add two rows to the "tables" -- one with the new name, and one to remove the old name
            //    XXX need to find the old name

            // XXX insert the new name with this oid
            tables_t->insert();

            // XXX insert the old name with exists = false
            tables_t->insert();
        } else {
            // 2) determine the set of changes between the provided schema and the prior schema
            auto old_schema = _schema_manager->get_schema(msg.oid, xid, lsn);
            auto update = _schema_manager->generate_update(old_schema, msg.columns, xid, lsn);

            _schema_manager->alter_schema(msg.oid, msg.columns[update.position], update);

            // 5) XXX if there's a primary key change, what do we do?  we'll need to re-build the entire table
        }
    }

    void
    TableManager::drop_table(uint64_t xid,
                             uint64_t lsn,
                             const PgMsgDropTable &msg)
    {
        // update the system tables to represent that the table was dropped from XID+LSN
        auto tables_t = get_table(TABLES_TABLE);

        // 1) update the "tables" with a drop entry
        //    note: the GC-3 should evict these entries once the data has been brought forward and
        //          we can assume the table_id won't be re-used until that operation is complete
        tuple = msg.oid, msg.schema, msg.table, xid, lsn, false;
        tables_t->insert(tuple);
    }
}
