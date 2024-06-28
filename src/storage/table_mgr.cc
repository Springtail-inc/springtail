#include <common/json.hh>
#include <common/properties.hh>

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
        // get the base directory for table data
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        Json::get_to<std::filesystem::path>(json, "table_dir", _table_base,
                                            "/opt/springtail/table");

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

        // retrieve the roots of the table
        auto &&roots = _find_roots(table_id, xid);

        // construct the table and return it
        auto schema = SchemaMgr::get_instance()->get_extent_schema(table_id, xid);
        return std::make_shared<Table>(table_id, xid, _table_base / fmt::format("{}", table_id),
                                       schema->get_sort_keys(), std::vector<std::vector<std::string>>{},
                                       roots, schema);
    }

    std::vector<uint64_t>
    TableMgr::_find_roots(uint64_t table_id,
                          uint64_t xid)
    {
        std::vector<uint64_t> roots;

        // get the root of the table's primary index
        auto roots_t = _get_system_table(sys_tbl::TableRoots::ID, xid);
        auto roots_key_fields = roots_t->extent_schema()->get_sort_fields();

        auto search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id, constant::INDEX_PRIMARY, xid);

        // XXX this won't work... need to find the inverse_lower_bound()
        auto it = roots_t->lower_bound(search_key);
        if (it == roots_t->end() || !FieldTuple(roots_key_fields, *it).equal(*search_key)) {
            // no roots?  try to find it in the roots file by returning empty roots
        } else {
            // retrieve the root extent ID of the primary
            auto eid_f = roots_t->extent_schema()->get_field("extent_id");
            roots.push_back(eid_f->get_uint64(*it));
        }

        return roots;
    }

    MutableTablePtr
    TableMgr::get_mutable_table(uint64_t table_id,
                                uint64_t access_xid,
                                uint64_t target_xid)
    {
        boost::shared_lock lock(_mutex);

        // check the system tables
        auto table = _get_mutable_system_table(table_id, access_xid, target_xid);
        if (table != nullptr) {
            return table;
        }

        // retrieve the roots of the table
        auto &&roots = _find_roots(table_id, access_xid);

        // construct the mutable table and return it
        auto schema = SchemaMgr::get_instance()->get_extent_schema(table_id, access_xid);
        return std::make_shared<MutableTable>(table_id, access_xid, target_xid, roots,
                                              _table_base / fmt::format("{}", table_id),
                                              schema->get_sort_keys(),
                                              std::vector<std::vector<std::string>>{},
                                              schema);
    }

    void
    TableMgr::create_table(uint64_t xid,
                           uint64_t lsn,
                           const PgMsgTable &msg)
    {
        // XXX we need to think if it's safe to just use the previous XID as the access XID when performing these operations
        uint64_t access_xid = xid - 1;

        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Creating table {}.{}@{} {} - {}", msg.schema, msg.table, xid, msg.oid, lsn);

        // 1) add a table -> name mapping that starts the table at the given XID/LSN
        auto table_names_t = get_mutable_table(sys_tbl::TableNames::ID, access_xid, xid);
        auto tuple = sys_tbl::TableNames::Data::tuple(msg.schema, msg.table, msg.oid, xid, lsn, true);
        table_names_t->insert(tuple, xid, constant::UNKNOWN_EXTENT);
        table_names_t->finalize();

        // add a table_id -> UNKNOWN extent ID mapping that indicates an empty primary index at the given XID/LSN

        // note: we perform an upsert() because the table records at the XID level and there may
        //       have been an earlier DROP in the same XID, and we can't insert the same entry twice
        auto table_roots_t = get_mutable_table(sys_tbl::TableRoots::ID, access_xid, xid);
        tuple = sys_tbl::TableRoots::Data::tuple(msg.oid, 0, xid, constant::UNKNOWN_EXTENT);
        table_roots_t->upsert(tuple, xid, constant::UNKNOWN_EXTENT);
        table_roots_t->finalize();

        // track the primary index keys using an ordered map of position -> column_id
        std::map<uint32_t, uint32_t> primary_keys;

        // 2) add a row to "schemas" for each column
        auto schemas_t = get_mutable_table(sys_tbl::Schemas::ID, access_xid, xid);
        for (auto &&column : msg.columns) {
            // insert an entry into the schemas table
            auto tuple = sys_tbl::Schemas::Data::tuple(msg.oid, column.position, xid, lsn,
                                                       true, // exists
                                                       column.column_name,
                                                       static_cast<uint8_t>(_convert_pg_type(column.udt_type)),
                                                       column.is_nullable, column.default_value,
                                                       static_cast<uint8_t>(SchemaUpdateType::NEW_COLUMN));
            schemas_t->insert(tuple, xid, constant::UNKNOWN_EXTENT);

            // record the primary key columns and order
            if (column.is_pkey) {
                primary_keys[column.pk_position] = column.position;
            }
        }
        schemas_t->finalize();

        // 3) if there's a primary key, add a row to "indexes" for each key column of the primary key
        if (!primary_keys.empty()) {
            auto indexes_t = get_mutable_table(sys_tbl::Indexes::ID, access_xid, xid);
            auto fields = sys_tbl::Indexes::Data::fields(msg.oid, 0, xid, 0, 0);

            for (auto &&entry : primary_keys) {
                fields->at(sys_tbl::Indexes::Data::POSITION) = std::make_shared<ConstTypeField<uint32_t>>(entry.first);
                fields->at(sys_tbl::Indexes::Data::COLUMN_ID) = std::make_shared<ConstTypeField<uint32_t>>(entry.second);

                indexes_t->insert(std::make_shared<FieldTuple>(fields, nullptr), xid, constant::UNKNOWN_EXTENT);
            }
            indexes_t->finalize();
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
        // XXX we need to think if it's safe to just use the previous XID as the access XID when performing these operations
        uint64_t access_xid = xid - 1;

        // retrieve the current name of the table
        auto &&old_name = _get_table_name(msg.oid, xid, lsn);

        if (old_name.first != msg.schema || old_name.second != msg.table) {
            // 1) if the table name is changed, add two rows to the "tables" -- one with the new name, and one to remove the old name
            auto table_names_t = get_mutable_table(sys_tbl::TableNames::ID, access_xid, xid);

            // insert the new name with this oid
            auto new_tuple = sys_tbl::TableNames::Data::tuple(msg.schema, msg.table, msg.oid, xid, lsn, true);
            table_names_t->insert(new_tuple, xid, constant::UNKNOWN_EXTENT);

            // insert the old name with exists = false
            auto old_tuple = sys_tbl::TableNames::Data::tuple(old_name.first, old_name.second, msg.oid, xid, lsn, false);
            table_names_t->insert(old_tuple, xid, constant::UNKNOWN_EXTENT);

            // sync the changes to disk
            table_names_t->finalize();
        } else {
            // 2) determine the set of changes between the provided schema and the prior schema
            auto &&old_columns = SchemaMgr::get_instance()->get_columns(msg.oid, xid, lsn);

            std::map<uint32_t, SchemaColumn> new_columns;
            for (const auto &col : msg.columns) {
                new_columns[col.position] = SchemaColumn(xid, lsn, col.column_name, col.position,
                                                         _convert_pg_type(col.udt_type),
                                                         true, col.is_nullable,
                                                         col.is_pkey ? col.pk_position : std::optional<uint32_t>(),
                                                         col.default_value);
            }

            auto update = SchemaMgr::get_instance()->generate_update(old_columns, new_columns, xid, lsn);
            auto &col = new_columns[update.position];

            // apply the changes to the schemas table
            auto schemas_t = get_mutable_table(sys_tbl::Schemas::ID, access_xid, xid);
            auto tuple = sys_tbl::Schemas::Data::tuple(msg.oid, update.position, xid, lsn,
                                                       (update.update_type != SchemaUpdateType::REMOVE_COLUMN), // exists
                                                       col.name, static_cast<uint8_t>(col.type),
                                                       col.nullable, col.default_value, static_cast<uint8_t>(update.update_type));
            schemas_t->insert(tuple, xid, constant::UNKNOWN_EXTENT);

            // sync the changes to disk
            schemas_t->finalize();

            // XXX notify the schema manager somehow to update it's cached schema information?

            // 5) XXX if there's a primary key change, what do we do?  we'll need to re-build the entire table
        }
    }

    void
    TableMgr::drop_table(uint64_t xid,
                         uint64_t lsn,
                         const PgMsgDropTable &msg)
    {
        // XXX we need to think if it's safe to just use the previous XID as the access XID when performing these operations
        uint64_t access_xid = xid - 1;

        // 1) update the "table_names" with a drop entry
        //    note: the GC-3 should evict these entries once the data has been brought forward and
        //          we can assume the table_id won't be re-used until that operation is complete
        auto table_names_t = get_mutable_table(sys_tbl::TableNames::ID, access_xid, xid);
        auto tuple = sys_tbl::TableNames::Data::tuple(msg.schema, msg.table, msg.oid, xid, lsn, false);
        table_names_t->insert(tuple, xid, constant::UNKNOWN_EXTENT);
        table_names_t->finalize();

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
                                           schema);
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
                                           schema);
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
                                           schema);
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
                                           schema);
        }
        default:
            return nullptr;
        }
    }

    void
    TableMgr::update_roots(uint64_t table_id,
                           uint64_t access_xid,
                           uint64_t target_xid,
                           const std::vector<uint64_t> &roots)
    {
        // modify the table_roots table
        auto roots_t = get_mutable_table(sys_tbl::TableRoots::ID, access_xid, target_xid);
        for (uint64_t idx = 0; idx < roots.size(); ++idx) {
            auto tuple = sys_tbl::TableRoots::Data::tuple(table_id, idx, target_xid, roots[idx]);
            roots_t->upsert(tuple, target_xid, constant::UNKNOWN_EXTENT);

            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Updated root {}@{} {} - {}", table_id, target_xid, idx, roots[idx]);
        }

        // finalize the changes to disk
        roots_t->finalize();
    }


    MutableTablePtr
    TableMgr::_get_mutable_system_table(uint64_t table_id,
                                        uint64_t access_xid,
                                        uint64_t target_xid)
    {
        // initialize the system tables using the look-aside root files
        std::vector<uint64_t> roots;
        std::vector<std::vector<std::string>> secondary_keys;
        ExtentSchemaPtr schema;
        std::filesystem::path table_path;

        switch (table_id) {
        case (sys_tbl::TableNames::ID): {
            secondary_keys.push_back(sys_tbl::TableNames::Secondary::KEY);
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::TableNames::ID, access_xid);
            table_path = _table_base / std::to_string(sys_tbl::TableNames::ID);

            return std::make_shared<MutableTable>(sys_tbl::TableNames::ID,
                                                  access_xid,
                                                  target_xid,
                                                  roots,
                                                  table_path,
                                                  sys_tbl::TableNames::Primary::KEY,
                                                  secondary_keys,
                                                  schema);
        }
        case (sys_tbl::TableRoots::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::TableRoots::ID, access_xid);
            table_path = _table_base / std::to_string(sys_tbl::TableRoots::ID);

            return std::make_shared<MutableTable>(sys_tbl::TableRoots::ID,
                                                  access_xid,
                                                  target_xid,
                                                  roots,
                                                  table_path,
                                                  sys_tbl::TableRoots::Primary::KEY,
                                                  secondary_keys,
                                                  schema);
        }
        case (sys_tbl::Indexes::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::Indexes::ID, access_xid);
            table_path = _table_base / std::to_string(sys_tbl::Indexes::ID);

            return std::make_shared<MutableTable>(sys_tbl::Indexes::ID,
                                                  access_xid,
                                                  target_xid,
                                                  roots,
                                                  table_path,
                                                  sys_tbl::Indexes::Primary::KEY,
                                                  secondary_keys,
                                                  schema);
        }
        case (sys_tbl::Schemas::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(sys_tbl::Schemas::ID, access_xid);
            table_path = _table_base / std::to_string(sys_tbl::Schemas::ID);

            return std::make_shared<MutableTable>(sys_tbl::Schemas::ID,
                                                  access_xid,
                                                  target_xid,
                                                  roots,
                                                  table_path,
                                                  sys_tbl::Schemas::Primary::KEY,
                                                  secondary_keys,
                                                  schema);
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
        auto row_i = table_names_t->index(1)->inverse_upper_bound(search_key);
        if (row_i == table_names_t->index(1)->end() ||
            schema->get_field("table_id")->get_uint64(*row_i) != table_id ||
            schema->get_field("exists")->get_bool(*row_i) == false) {
            // XXX error -- table_id doesn't exist at the provided XID
        }

        // read the row from the extent and retrieve the FQN
        auto secondary_fields = table_names_t->index(1)->get_schema()->get_fields();
        auto extent_id = secondary_fields->at(sys_tbl::TableNames::Secondary::EXTENT_ID)->get_uint64(*row_i);
        auto row_id = secondary_fields->at(sys_tbl::TableNames::Secondary::ROW_ID)->get_uint32(*row_i);
        auto page = table_names_t->read_page(extent_id);
        auto row = page->at(row_id);

        std::string &&old_name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(*row);
        std::string &&old_namespace = fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(*row);
        return { old_namespace, old_name };
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
