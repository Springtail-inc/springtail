#include <storage/table_mgr.hh>
#include <storage/system_tables.hh>

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

    TableMgr::TableMgr()
    {
        // XXX set the cache sizes and base path from global properties
        _read_cache = std::make_shared<LruObjectCache<std::pair<std::filesystem::path, uint64_t>, Extent>>(64 * 1024 * 1024);
        _write_cache = MutableBTree::create_cache(64 * 1024 * 1024);
        _data_cache = std::make_shared<DataCache>(true);
        _table_base = "/tmp/table";

        // make sure that the base directory for tables exists
        std::filesystem::create_directories(_table_base);
    }

    TablePtr
    TableMgr::get_table(uint64_t table_id,
                        uint64_t xid,
                        uint64_t lsn)
    {
        boost::shared_lock lock(_mutex);

        // check the system tables
        auto table = _get_system_table(table_id, xid);
        if (table != nullptr) {
            return table;
        }

        throw StorageError();

#if 0
        // check the table cache
        // XXX Table vs. Mutable Table?  How to handle XID in Table?
        auto table = _table_cache.get(table_id);
        if (table != nullptr) {
            return table;
        }

        // read the table metadata from the appropriate system table
        auto table_roots_t = _system_tables.find(sys_tbl::TableRoots::ID)->second;
        auto search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id, 0, xid);

        // XXX specialized search that finds the entry directly before the lower_bound(search_key)
        auto pos_i = table_roots_t->primary->inverted_upper_bound(search_key);
        if (pos_i == table_roots_t->primary->end()) {
            return nullptr;
        }

        auto schema = table_roots_t->get_schema(xid, lsn);
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
        _table_cache.insert(table_id, table);
        return table;
#endif
    }

    MutableTablePtr
    TableMgr::get_mutable_table(uint64_t table_id,
                                uint64_t xid)
    {
        boost::shared_lock lock(_mutex);

        // check the system tables
        auto table = _get_mutable_system_table(table_id, xid);
        if (table != nullptr) {
            return table;
        }

        throw StorageError();
    }

    void
    TableMgr::create_table(uint64_t xid,
                           uint64_t lsn,
                           const PgMsgTable &msg)
    {
        // add a table -> name mapping that starts the table at the given XID/LSN
        auto table_names_t = get_mutable_table(sys_tbl::TableNames::ID, xid);
        auto tuple = sys_tbl::TableNames::Data::tuple(msg.schema, msg.table, msg.oid, xid, lsn, true);
        table_names_t->insert(tuple, xid, lsn);

        // add a table_id -> nullptr extent ID mapping that indicates an empty primary index at the given XID/LSN
        // note: we perform an upsert because the table records at the XID level and there may have
        //       been an earlier DROP within the same XID, and we can't insert the same entry twice
        auto table_roots_t = get_mutable_table(sys_tbl::TableRoots::ID, xid);
        tuple = sys_tbl::TableRoots::Data::tuple(msg.oid, 0, xid);
        table_roots_t->upsert(tuple, xid, lsn);

        // track the primary index keys using an ordered map of position -> column_id
        std::map<uint32_t, uint32_t> primary_keys;

        // 2) add a row to "schemas" for each column
        auto schemas_t = get_mutable_table(sys_tbl::Schemas::ID, xid);
        for (auto &&column : msg.columns) {
            // insert an entry into the schemas table
            auto tuple = sys_tbl::Schemas::Data::tuple(msg.oid, column.position, xid, lsn,
                                                       true, // exists
                                                       column.column_name,
                                                       static_cast<uint8_t>(_convert_pg_type(column.udt_type)),
                                                       column.is_nullable, column.default_value,
                                                       static_cast<uint8_t>(SchemaUpdateType::NEW_COLUMN));
            schemas_t->insert(tuple, xid, lsn);

            // record the primary key columns and order
            if (column.is_pkey) {
                primary_keys[column.pk_position] = column.position;
            }
        }

        // 3) if there's a primary key, add a row to "indexes" for each key column of the primary key
        if (!primary_keys.empty()) {
            auto indexes_t = get_mutable_table(sys_tbl::Indexes::ID, xid);
            auto fields = sys_tbl::Indexes::Data::fields(msg.oid, 0, xid, 0, 0);

            for (auto &&entry : primary_keys) {
                fields->at(sys_tbl::Indexes::Data::POSITION) = std::make_shared<ConstTypeField<uint32_t>>(entry.first);
                fields->at(sys_tbl::Indexes::Data::COLUMN_ID) = std::make_shared<ConstTypeField<uint32_t>>(entry.second);
                
                indexes_t->insert(std::make_shared<FieldTuple>(fields, nullptr), xid, lsn);
            }
        }

        // XXX 4) Secondary indexes??  unclear when to create them.

        // XXX notify the schema manager of the new schema?

        // XXX create the table's directory?  create empty index files?
    }

    void
    TableMgr::alter_table(uint64_t xid,
                          uint64_t lsn,
                          const PgMsgTable &msg)
    {
        // retrieve the current name of the table
        auto &&old_name = _get_table_name(msg.oid, xid, lsn);

        if (old_name.first != msg.schema || old_name.second != msg.table) {
            // 1) if the table name is changed, add two rows to the "tables" -- one with the new name, and one to remove the old name
            auto table_names_t = get_mutable_table(sys_tbl::TableNames::ID, xid);

            // insert the new name with this oid
            auto new_tuple = sys_tbl::TableNames::Data::tuple(msg.schema, msg.table, msg.oid, xid, lsn, true);
            table_names_t->insert(new_tuple, xid, lsn);

            // insert the old name with exists = false
            auto old_tuple = sys_tbl::TableNames::Data::tuple(old_name.first, old_name.second, msg.oid, xid, lsn, false);
            table_names_t->insert(old_tuple, xid, lsn);
        } else {
            // 2) determine the set of changes between the provided schema and the prior schema
            auto &&old_columns = SchemaMgr::get_instance()->get_columns(msg.oid, xid, lsn);

            std::map<uint32_t, SchemaColumn> new_columns;
            for (const auto &col : msg.columns) {
                new_columns[col.position] = SchemaColumn(xid, lsn, col.column_name, col.position,
                                                         _convert_pg_type(col.udt_type),
                                                         true, col.is_nullable, col.default_value);
            }

            auto update = SchemaMgr::get_instance()->generate_update(old_columns, new_columns, xid, lsn);
            auto &col = new_columns[update.position];

            // apply the changes to the schemas table
            auto schemas_t = get_mutable_table(sys_tbl::Schemas::ID, xid);
            auto tuple = sys_tbl::Schemas::Data::tuple(msg.oid, update.position, xid, lsn,
                                                       (update.update_type != SchemaUpdateType::REMOVE_COLUMN), // exists
                                                       col.name, static_cast<uint8_t>(col.type),
                                                       col.nullable, col.default_value, static_cast<uint8_t>(update.update_type));
            schemas_t->insert(tuple, xid, lsn);

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
        auto table_names_t = get_mutable_table(sys_tbl::TableNames::ID, xid);
        auto tuple = sys_tbl::TableNames::Data::tuple(msg.schema, msg.table, msg.oid, xid, lsn, true);
        table_names_t->insert(tuple, xid, lsn);

        // note: we don't update the "table_roots" with a null entry because a truncate will utilize
        //       an empty extent as the root.  This allows us to maintain the reverse linking of
        //       historical roots.

        // XXX do we need to clear the schemas table at the XID/LSN?
        // XXX do we need to clear the indexes table at the XID/LSN?
    }

    TablePtr
    TableMgr::_get_system_table(uint64_t table_id,
                                uint64_t xid)
    {
        // initialize the system tables using the look-aside root files
        std::vector<uint64_t> roots;
        std::vector<std::vector<std::string>> secondary_keys;
        ExtentSchemaPtr schema;
        std::filesystem::path table_path;

        switch (table_id) {
        case (sys_tbl::TableNames::ID): {
            secondary_keys.push_back(sys_tbl::TableNames::Secondary::KEY);
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::TableNames::ID, xid);
            table_path = _table_base / std::to_string(sys_tbl::TableNames::ID);

            return std::make_shared<Table>(sys_tbl::TableNames::ID,
                                           xid,
                                           table_path,
                                           sys_tbl::TableNames::Primary::KEY,
                                           secondary_keys,
                                           roots,
                                           schema,
                                           _read_cache);
        }
        case (sys_tbl::TableRoots::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::TableRoots::ID, xid);
            table_path = _table_base / std::to_string(sys_tbl::TableRoots::ID);

            return std::make_shared<Table>(sys_tbl::TableRoots::ID,
                                           xid,
                                           table_path,
                                           sys_tbl::TableRoots::Primary::KEY,
                                           secondary_keys,
                                           roots,
                                           schema,
                                           _read_cache);
        }
        case (sys_tbl::Indexes::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::Indexes::ID, xid);
            table_path = _table_base / std::to_string(sys_tbl::Indexes::ID);

            return std::make_shared<Table>(sys_tbl::Indexes::ID,
                                           xid,
                                           table_path,
                                           sys_tbl::Indexes::Primary::KEY,
                                           secondary_keys,
                                           roots,
                                           schema,
                                           _read_cache);
        }
        case (sys_tbl::Schemas::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::Schemas::ID, xid);
            table_path = _table_base / std::to_string(sys_tbl::Schemas::ID);

            return std::make_shared<Table>(sys_tbl::Schemas::ID,
                                           xid,
                                           table_path,
                                           sys_tbl::Schemas::Primary::KEY,
                                           secondary_keys,
                                           roots,
                                           schema,
                                           _read_cache);
        }
        default:
            return nullptr;
        }
    }

    MutableTablePtr
    TableMgr::_get_mutable_system_table(uint64_t table_id,
                                        uint64_t xid)
    {
        // initialize the system tables using the look-aside root files
        std::vector<uint64_t> roots;
        std::vector<std::vector<std::string>> secondary_keys;
        ExtentSchemaPtr schema;
        std::filesystem::path table_path;

        switch (table_id) {
        case (sys_tbl::TableNames::ID): {
            secondary_keys.push_back(sys_tbl::TableNames::Secondary::KEY);
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::TableNames::ID, xid);
            table_path = _table_base / std::to_string(sys_tbl::TableNames::ID);

            return std::make_shared<MutableTable>(sys_tbl::TableNames::ID,
                                                  xid,
                                                  roots,
                                                  table_path,
                                                  sys_tbl::TableNames::Primary::KEY,
                                                  secondary_keys,
                                                  schema,
                                                  _data_cache,
                                                  _write_cache,
                                                  _read_cache);
        }
        case (sys_tbl::TableRoots::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::TableRoots::ID, xid);
            table_path = _table_base / std::to_string(sys_tbl::TableRoots::ID);

            return std::make_shared<MutableTable>(sys_tbl::TableRoots::ID,
                                                  xid,
                                                  roots,
                                                  table_path,
                                                  sys_tbl::TableRoots::Primary::KEY,
                                                  secondary_keys,
                                                  schema,
                                                  _data_cache,
                                                  _write_cache,
                                                  _read_cache);
        }
        case (sys_tbl::Indexes::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::Indexes::ID, xid);
            table_path = _table_base / std::to_string(sys_tbl::Indexes::ID);

            return std::make_shared<MutableTable>(sys_tbl::Indexes::ID,
                                                  xid,
                                                  roots,
                                                  table_path,
                                                  sys_tbl::Indexes::Primary::KEY,
                                                  secondary_keys,
                                                  schema,
                                                  _data_cache,
                                                  _write_cache,
                                                  _read_cache);
        }
        case (sys_tbl::Schemas::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::Schemas::ID, xid);
            table_path = _table_base / std::to_string(sys_tbl::Schemas::ID);

            return std::make_shared<MutableTable>(sys_tbl::Schemas::ID,
                                                  xid,
                                                  roots,
                                                  table_path,
                                                  sys_tbl::Schemas::Primary::KEY,
                                                  secondary_keys,
                                                  schema,
                                                  _data_cache,
                                                  _write_cache,
                                                  _read_cache);
        }
        default:
            return nullptr;
        }
    }

    std::pair<std::string, std::string>
    TableMgr::_get_table_name(uint64_t table_id,
                              uint64_t xid,
                              uint64_t lsn)
    {
        // XXX check a cache of the names?

        // XXX need to make sure that the system tables are flushed to use the read interface, otherwise we would need to have a way to search the MutableBTree
        // get "tables" system table
        auto table_names_t = get_table(sys_tbl::TableNames::ID, xid, lsn);
        auto schema = table_names_t->schema(xid);
        auto fields = schema->get_fields();

        auto search_key = sys_tbl::TableNames::Secondary::key_tuple(table_id, xid, lsn);

        // find the row that matches the name of the table_id at the given XID/LSN
        auto row_i = table_names_t->secondary(0)->inverse_upper_bound(search_key, xid);
        if (row_i == table_names_t->secondary(0)->end() ||
            schema->get_field("table_id")->get_uint64(*row_i) != table_id ||
            schema->get_field("exists")->get_bool(*row_i) == false) {
            // XXX error -- table_id doesn't exist at the provided XID
        }

        // read the row from the extent and retrieve the FQN
        auto secondary_fields = table_names_t->secondary(0)->get_schema()->get_fields();
        auto extent_id = secondary_fields->at(sys_tbl::TableNames::Secondary::EXTENT_ID)->get_uint64(*row_i);
        auto row_id = secondary_fields->at(sys_tbl::TableNames::Secondary::ROW_ID)->get_uint64(*row_i);
        auto extent = table_names_t->read_extent(extent_id);
        auto row = extent->at(row_id);

        std::string &&old_name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(row);
        std::string &&old_namespace = fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(row);
        return { old_name, old_namespace };
    }

    SchemaType
    TableMgr::_convert_pg_type(const std::string &pg_type)
    {
        if (pg_type == "int4" || pg_type == "int" || pg_type == "serial4" || pg_type == "serial" ||
            pg_type == "date") {
            return SchemaType::INT32;
        }

        if (pg_type == "text" || pg_type == "varchar" || pg_type == "bpchar" || pg_type == "_bpchar") {
            return SchemaType::TEXT;
        }

        if (pg_type == "int8" || pg_type == "serial8" ||
            pg_type == "timestamp" || pg_type == "timestamptz" ||
            pg_type == "time" || pg_type == "timetz") {
            return SchemaType::INT64;
        }

        if (pg_type == "bool") {
            return SchemaType::BOOLEAN;
        }

        if (pg_type == "int2" || pg_type == "serial2") {
            return SchemaType::INT16;
        }

        if (pg_type == "float4") {
            return SchemaType::FLOAT32;
        }

        if (pg_type == "float8") {
            return SchemaType::FLOAT64;
        }

        if (pg_type == "float8") {
            return SchemaType::FLOAT64;
        }

        // note: we currently fail on unknown types, eventually stuff them into a binary
        assert(false);
    }
}
