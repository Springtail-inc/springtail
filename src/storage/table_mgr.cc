#include <storage/table_mgr.hh>

namespace springtail {

    /* static member initialization must happen outside of class */
    TableMgr* TableMgr::_instance {nullptr};
    boost::mutex TableMgr::_instance_mutex;

    TableMgr *
    TableMgr::get_instance()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new TableMgr();
        }

        return _instance;
    }
    
    void
    TableMgr::shutdown()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }


    TablePtr
    TableMgr::get_table(uint64_t table_id,
                        uint64_t xid,
                        uint64_t lsn)
    {
        boost::shared_lock lock(_mutex);

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
        auto table_roots_t = _system_tables.find(sys_tbl::TableRoots::ID)->second;
        auto search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id, 0, xid);

        // XXX specialized search that finds the entry directly before the lower_bound(search_key)
        auto pos_i = tables_t->primary->inverted_upper_bound(search_key);
        if (pos_i == tables_t->primary->end()) {
            return nullptr;
        }

        auto schema = tables_t->get_schema(xid, lsn);
        auto fields = schema->get_fields("table_id", "xid", "lsn");

        // verify that the returned entry references the requested table
        if (fields->at(0)->get_uint64(*pos_i) != table_id) {
            // if not, then the table does not exist at the provided XID/LSN
            return nullptr;
        }

        auto table = std::make_shared<Table>(table_id,
                                             _cache,
                                             fields->at(XID)->get_uint64(*pos_i),
                                             fields->at(EXTENT_ID)->get_uint64(*pos_i));
        return table;
    }

    void
    TableMgr::create_table(uint64_t xid,
                           uint64_t lsn,
                           const PgMsgTable &msg)
    {
        // add a table -> name mapping that starts the table at the given XID/LSN
        auto table_names_t = get_table(sys_tbl::TableNames::ID);
        auto tuple = sys_tbl::TableNames::Data::tuple(msg.schema, msg.table, msg.oid, xid, lsn, true);
        table_names_t->insert(tuple);

        // add a table_id -> nullptr extent ID mapping that indicates an empty primary index at the given XID/LSN
        // note: we perform an upsert because the table records at the XID level and there may have
        //       been an earlier DROP within the same XID, and we can't insert the same entry twice
        auto table_roots_t = get_table(sys_tbl::TableRoots::ID);
        tuple = sys_tbl::TableRoots::Data::tuple(msg.oid, 0, xid);
        table_roots_t->upsert(tuple);

        // track the primary index keys using an ordered map of position -> column_id
        std::map<uint32_t, uint32_t> primary_keys;

        // 2) add a row to "schemas" for each column
        for (auto &&column : msg.columns) {
            // XXX need to map the udt_type
            SchemaColumn scolumn(xid, lsn, column.column_name, column.position, column.udt_type, column.is_nullable, column.default_value);
            SchemaUpdate update(xid, lsn, SchemaUpdateType::ADD_COLUMN, column.position, column.column_name, column.udt_type, column.is_nullable, column.default_value);

            _schema_manager->alter_schema(msg.oid, scolumn, update);

            // record the primary key columns and order
            if (column.is_pkey) {
                primary_keys[column.pk_position] = column.position;
            }
        }

        // 3) if there's a primary key, add a row to "indexes" for each key column of the primary key
        if (!primary_keys.empty()) {
            auto indexes_t = get_table(sys_tbl::Indexes::ID);
            auto fields = sys_tbl::Indexes::Data::fields(msg.oid, 0, xid, 0, 0);

            for (auto &&entry : primary_keys) {
                fields->at(sys_tbl::Indexes::Data::POSITION) = std::make_shared<ConstTypeField<uint32_t>>(entry.first);
                fields->at(sys_tbl::Indexes::Data::COLUMN_ID) = std::make_shared<ConstTypeField<uint32_t>>(entry.second);

                indexes_t->insert(FieldTuple(fields, nullptr));
            }
        }

        // XXX 4) Secondary indexes??  unclear when to create them.
    }

    void
    TableMgr::alter_table(uint64_t xid,
                          uint64_t lsn,
                          const PgMsgTable &msg)
    {
        // get "tables" system table
        auto table_names_t = get_table(TablesNames::ID);
        auto schema = table_names_t->schema(xid);
        auto fields = schema->get_fields();

        auto search_key = sys_tbl::TableNames::Secondary::key_tuple(msg.oid, xid, lsn);

        // find the row that matches the name of the table_id at the given XID/LSN
        auto row_i = tables_t->secondary[0]->inverse_upper_bound(search_key);
        if (row_i == tables_t->secondary[0]->end() ||
            table_id_f->get_uint64(*row_i) != msg.oid) {
            // XXX error -- table_id doesn't exist at the provided XID
        }

        // read the row from the extent and retrieve the FQN
        auto secondary_fields = table_names_t->secondary[0]->get_fields();
        auto extent_id = secondary_fields->at(sys_tbl::TableNames::Secondary::EXTENT_ID)->get_uint64(*pos_i);
        auto row_id = seconary_fields->at(sys_tbl::TableNames::Secondary::ROW_ID)->get_uint64(*pos_i);
        auto extent = _read_extent(extent_id);
        auto row = extent->at(row_id);

        std::string old_name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row);
        std::string old_namespace = fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(row);

        if (old_namespace != msg.schema || old_name != msg.table) {
            // 1) if the table name is changed, add two rows to the "tables" -- one with the new name, and one to remove the old name

            // insert the new name with this oid
            auto new_tuple = sys_tbl::TableNames::Data::tuple(msg.schema, msg.table, msg.oid, xid, lsn, true);
            table_names_t->insert(new_tuple);

            // insert the old name with exists = false
            auto old_tuple = sys_tbl::TableNames::Data::tuple(old_namespace, old_name, msg.oid, xid, lsn, false);
            tables_t->insert(old_tuple);
        } else {
            // 2) determine the set of changes between the provided schema and the prior schema
            auto old_schema = _schema_manager->get_schema(msg.oid, xid, lsn);
            auto update = _schema_manager->generate_update(old_schema, msg.columns, xid, lsn);
            auto &&col = msg.columns[update.position]; // XXX position may not match index offset

            // apply the changes to the schemas table
            auto schemas_t = get_table(sys_tbl::Schemas::ID);
            auto tuple = sys_tbl::Schemas::Data::tuple(msg.oid, xid, lsn, col.name, col.position, col.type,
                                                       (update.update_type != SchemaUpdateType::REMOVE_COLUMN), // exists
                                                       col.nullable, col.default_value, update.update_type);
            schemas_t->insert(tuple);

            // XXX notify the schema manager somehow to update it's cached schema information?

            // 5) XXX if there's a primary key change, what do we do?  we'll need to re-build the entire table
        }
    }

    void
    TableMgr::drop_table(uint64_t xid,
                         uint64_t lsn,
                         const PgMsgDropTable &msg)
    {
        // 1) update the "table_names" with a drop entry
        //    note: the GC-3 should evict these entries once the data has been brought forward and
        //          we can assume the table_id won't be re-used until that operation is complete
        auto table_names_t = get_table(sys_tbl::TableNames::ID);
        auto tuple = sys_tbl::TableNames::Data::tuple(msg.schema, msg.table, msg.oid, xid, lsn, true);
        table_names_t->insert(tuple);

        // note: we don't update the "table_roots" with a null entry because a truncate will utilize
        //       an empty extent as the root.  This allows us to maintain the reverse linking of
        //       historical roots.

        // XXX do we need to clear the schemas table at the XID/LSN?
        // XXX do we need to clear the indexes table at the XID/LSN?
    }
}
