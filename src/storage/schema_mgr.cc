#include <storage/exception.hh>
#include <storage/schema.hh>
#include <storage/schema_manager.hh>

namespace springtail {
    /* static member initialization must happen outside of class */
    SchemaMgr* SchemaMgr::_instance {nullptr};
    boost::mutex SchemaMgr::_instance_mutex;

    SchemaMgr *
    SchemaMgr::get_instance()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new SchemaMgr();
        }

        return _instance;
    }
    
    void
    SchemaMgr::shutdown()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }


    std::map<uint32_t, SchemaColumn>
    SchemaMgr::SchemaInfo::_get_columns_for_xid(uint64_t xid)
    {
        std::map<uint32_t, SchemaColumn> columns;

        // for each column, try to find a valid SchemaColumn for the provided xid
        for (const auto &pair : _column_map) {
            // find the schema column definition for this XID
            auto &&i = pair.second.upper_bound(xid);

            // if there's no entry for this column with a starting XID < xid, go to the next column
            if (i == pair.second.end()) {
                continue;
            }

            // if the entry does not exist at this point, move to the next column
            if (!i->second.exists) {
                continue;
            }

            // we found a valid entry, add it to the vector of columns
            columns[pair.first] = i->second;
        }

        return columns;
    }

    void
    SchemaMgr::SchemaInfo::_read_schema_table(uint64_t table_id)
    {
        // first get the snapshots from the schemas table
        auto schemas_t = TableMgr::get_table(sys_tbl::Schemas::ID);

        // construct the column accessors for the schemas table
        auto schema = schemas_table->get_schema();
        auto fields = schema->get_fields();

        // read everything with the given table_id
        auto search_key = sys_tbl::Schemas::Data::tuple(table_id, 0, 0);

        // scan for the results of the schemas table
        auto table_i = schemas_t->lower_bound(search_key);
        while (table_i != schemas_t->end()) {
            Extent::Row &row = *table_i;

            // get the table_id from the entry
            uint64_t tid = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(row);
            if (tid != table_id) {
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            // construct a SchemaColumn for each row
            SchemaColumn column;
            column.xid = fields->at(sys_tbl::Schemas::Data::XID)->get_uint64(row);
            column.lsn = fields->at(sys_tbl::Schemas::Data::LSN)->get_uint64(row);
            column.name = fields->at(sys_tbl::Schemas::Data::NAME)->get_text(row);
            column.position = fields->at(sys_tbl::Schemas::Data::POSITION)->get_uint32(row);
            column.type = fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(row);
            column.nullable = fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(row);
            if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(row)) {
                column.default_value = fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(row);
            }
            column.update_type = fields->at(sys_tbl::Schemas::Data::UPDATE_TYPE)->get_uint8(row);

            // place the column into the map
            _column_map[column.position][column.xid].push_back(column);
        }
    }

    void
    SchemaMgr::SchemaInfo::_read_indexes_table(uint64_t table_id)
    {
        // get the "indexes" table
        auto indexes_t = TableManger::get_table(sys_tbl::Indexes::ID);
        auto schema = indexes_t->get_schema();
        auto fields = schema->get_fields();

        // read everything with the given table_id
        // note: use null entries for additional columns in the primary key to ensure we see all entries
        auto search_key = sys_tbl::Indexes::Primary::key_tuple(table_id, 0, 0);

        // scan for the results of the schemas_history table
        auto table_i = indexes_t->primary->lower_bound(search_key);
        while (table_i != schemas_history->end()) {
            Extent::Row &row = *table_i;

            // get the table_id from the entry
            uint64_t tid = fields->at(sys_tbl::Indexes::Data::TABLE_ID)->get_uint64(row);
            if (tid != table_id) {
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            uint64_t index_id = field->at(sys_tbl::Indexes::Data::INDEX_ID)->get_uint64(row);
            uint64_t xid = field->at(sys_tbl::Indexes::Data::XID)->get_uint64(row);
            uint32_t position = field->at(sys_tbl::Indexes::Data::LSN)->get_uint32(row);
            uint32_t column_id = field->at(sys_tbl::Indexes::Data::LSN)->get_uint32(row);

            if (index_id == 0) {
                _primary_index[xid].push_back(column_id);
            } else {
                _secondary_indexes[index_id][xid].push_back(column_id);
            }
        }
    }

    SchemaMgr::SchemaInfo::SchemaInfo(uint64_t table_id)
    {
        _read_schema_table(table_id);
        _read_indexes_table(table_id);
    }

    std::vector<std::string>
    SchemaMgr::SchemaInfo::get_primary_key(uint64_t xid)
    {
        std::vector<std::string> key;

        auto &&i = _primary_index.lower_bound(xid);
        for (uint32_t column : i->second) {
            // XXX need to handle lookup failure
            key.push_back(_column_map[column].lower_bound(xid)->second.name);
        }
    }

    std::shared_ptr<ExtentSchema>
    SchemaMgr::SchemaInfo::get_extent_schema(uint64_t extent_xid)
    {
        // determine the columns for the extent XID
        auto &&columns = _get_columns_for_xid(extent_xid);

        // use the vector of columns to generate the schema
        return std::make_shared<ExtentSchema>(columns);
    }

    std::shared_ptr<VirtualSchema>
    SchemaMgr::SchemaInfo::get_virtual_schema(uint32_t extent_xid,
                                              uint64_t target_xid,
                                              uint64_t lsn=0)
    {
        // determine the columns for the extent XID
        auto &&columns = _get_columns_for_xid(extent_xid);

        // now go through all of the updates for every column between the extent XID and the target
        // XID and apply those changes to construct the virtual schema
        for (const auto & [id, history]: _column_map) {
            // find any updates from xids after the extent schema xid
            // note: the map is in reverse XID order, so most recent XID first
            auto &&i = history.upper_bound(extent_xid);
            auto changes_i = std::make_reverse_iterator(i); // go through the map backward from the "current" entry

            while (true) {
                // no more updates, so continue to the next column
                if (changes_i == history.rend()) {
                    break;
                }

                // if any such updates are past the target_xid, then continue to the next column
                if (changes_i.first > target_xid) {
                    break;
                }

                // apply any updates from the xid prior to the LSN (if one was provided)
                for (const auto &update : changes_i.second) {
                    // if an LSN was provided and the update's LSN is greater than the provided LSN, stop applying updates
                    if (lsn && update.lsn > lsn) {
                        break;
                    }

                    // apply the update to the schema as a schema upgrade
                    // XXX this doesn't match with the creation of the schema below
                    schema->apply_update(update);
                }

                // move to the next xid with schema updates
                ++changes_i;
            }
        }

        // first create the extent schema
        auto schema = std::make_shared<ExtentSchema>(columns);

        // then layer the virtual schema
        return std::make_shared<VirtualSchema>(schema, columns, updates);
    }

    SchemaMgr::SchemaMgr() {
        _system_cache[sys_tbl::TableNames::ID] = std::make_shared<ExtentSchema>(sys_tbl::TableNames::SCHEMA);
        _system_cache[sys_tbl::TableRoots::ID] = std::make_shared<ExtentSchema>(sys_tbl::TableRoots::SCHEMA);
        _system_cache[sys_tbl::Indexes::ID] = std::make_shared<ExtentSchema>(sys_tbl::Indexes::SCHEMA);
        _system_cache[sys_tbl::Schemas::ID] = std::make_shared<ExtentSchema>(sys_tbl::Schemas::SCHEMA);
    }

    std::shared_ptr<const Schema>
    SchemaMgr::get_schema(uint64_t table_id, uint64_t extent_xid, uint64_t target_xid, uint64_t lsn = 0)
    {
        // first check if it's an immutable system table schema
        auto &&system_i = _system_cache.find(table_id);
        if (system_i != _system_cache.end()) {
            return system_i.second;
        }

        // next check the cache
        std::shared_ptr<SchemaInfo> info = _cache.get(table_id);

        // if not in the cache then read it and insert it into the cache
        if (info == nullptr) {
            info = std::make_shared<SchemaInfo>(table_id);
            _cache.insert(table_id, info);
        }

        // XXX get the extent schema first and then return that if extent_xid == target_xid?

        // retrieve the schema at the appropriate point-in-time
        return info->get_virtual_schema(extent_xid, target_xid, lsn);
    }

    std::shared_ptr<const ExtentSchema>
    SchemaMgr::get_extent_schema(uint64_t table_id,
                                 uint64_t xid)
    {
        // first check if it's an immutable system table schema
        auto &&system_i = _system_cache.find(table_id);
        if (system_i != _system_cache.end()) {
            return system_i.second;
        }

        // next check the cache
        std::shared_ptr<SchemaInfo> info = _cache.get(table_id);

        // if not in the cache then read it and insert it into the cache
        if (info == nullptr) {
            info = std::make_shared<SchemaInfo>(table_id);
            _cache.insert(table_id, info);
        }

        // retrieve the schema at the appropriate point-in-time
        return info->get_extent_schema(xid);
    }

    SchemaColumn
    SchemaMgr::generate_update(const std::map<uint32_t, SchemaColumn> &old_schema,
                               const std::map<uint32_t, SchemaColumn> &new_schema,
                               uint64_t xid,
                               uint64_t lsn)
    {
        SchemaColumn update;
        update.xid = xid;
        update.lsn = lsn;

        // if the old schema has more columns, then a column was removed
        if (old_schema.size() > new_schema.size()) {
            // find the missing column
            for (auto &&old_i : old_schema) {
                auto &&new_i = new_schema.find(old_i.first);
                if (new_i == new_schema.end()) {
                    // generate a REMOVE_COLUMN update
                    update.update_type = SchemaUpdateType::REMOVE_COLUMN;
                    update.position = old_i.second.position;

                    return update;
                }
            }
        }

        // if the old schema has fewer columns, then a column was added
        if (old_schema.size() < new_schema.size()) {
            // find the missing column
            for (auto &&new_i : new_schema) {
                auto &&old_i = old_schema.find(old_i.first);
                if (old_i == old_schema.end()) {
                    // generate an ADD_COLUMN update
                    update.update_type = SchemaUpdateType::ADD_COLUMN;
                    update.position = new_i.second.position;
                    update.name = new_i.second.name;
                    update.type = new_i.second.type;
                    update.nullable = new_i.second.nullable;
                    update.default_value = new_i.second.default_value;

                    return update;
                }
            }
        }

        // otherwise, compare each column to find the one with the difference
        for (auto &&old_i : old_schema) {
            // find the same column in the new schema
            new_i = new_schema.find(old_i.first);

            // it must exist or else the new and old schema are more than one modification apart
            assert(new_i != new_schema.end());

            // check for differences
            if (old_i.second.name != new_i.second.name) {
                // generate a NAME_CHANGE update
                update.update_type = SchemaUpdateType::NAME_CHANGE;
                update.position = new_i.second.position;
                update.name = new_i.second.name;

                return update;
            }

            if (old_i.second.nullable != new_i.second.nullable) {
                // generate a NULLABLE_CHANGE update
                update.update_type = SchemaUpdateType::NULLABLE_CHANGE;
                update.position = new_i.second.position;
                update.nullable = new_i.second.nullable;

                return update;
            }

            if (old_i.second.type != new_i.second.type) {
                // generate a TYPE_CHANGE update
                update.update_type = SchemaUpdateType::TYPE_CHANGE;
                update.position = new_i.second.position;
                update.type = new_i.second.type;

                return update;
            }
        }
    }
}
