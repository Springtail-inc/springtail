#include <storage/exception.hh>
#include <storage/schema.hh>
#include <storage/schema_mgr.hh>
#include <storage/system_tables.hh>
#include <storage/table_mgr.hh>
#include <storage/constants.hh>

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

            // get the last column definition within the XID
            auto &column = i->second.back();

            // if the entry does not exist at this point, move to the next column
            if (!column.exists) {
                continue;
            }

            // we found a valid entry, add it to the vector of columns
            columns[pair.first] = column;
        }

        return columns;
    }

    std::map<uint32_t, SchemaColumn>
    SchemaMgr::SchemaInfo::get_columns_for_xid_lsn(uint64_t xid, uint64_t lsn)
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

            // find the correct entry based on the requested LSN
            auto &&j = std::lower_bound(i->second.begin(), i->second.end(), lsn,
                                        [](const SchemaColumn &column, uint64_t lsn) {
                                            return (column.lsn < lsn);
                                        });
            auto &column = *j;

            // if the entry does not exist at this point, move to the next column
            if (!column.exists) {
                continue;
            }

            // we found a valid entry, add it to the vector of columns
            columns[pair.first] = column;
        }

        return columns;
    }

    void
    SchemaMgr::SchemaInfo::_read_schema_table(uint64_t table_id)
    {
        // first get the snapshots from the schemas table
        auto schemas_t = TableMgr::get_instance()->get_table(sys_tbl::Schemas::ID,
                                                             constant::LATEST_XID,
                                                             constant::MAX_LSN);

        // construct the column accessors for the schemas table
        auto schema = schemas_t->extent_schema();
        auto fields = schema->get_fields();

        // read everything with the given table_id
        auto search_key = sys_tbl::Schemas::Primary::key_tuple(table_id, 0, 0, 0);

        // scan for the results of the schemas table
        auto table_i = schemas_t->lower_bound(search_key);
        while (table_i != schemas_t->end()) {
            auto &row = *table_i;

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
            column.type = static_cast<SchemaType>(fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(row));
            column.exists = fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(row);
            column.nullable = fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(row);
            if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(row)) {
                column.default_value = fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(row);
            }
            column.update_type = static_cast<SchemaUpdateType>(fields->at(sys_tbl::Schemas::Data::UPDATE_TYPE)->get_uint8(row));

            // place the column into the map
            _column_map[column.position][column.xid].push_back(column);

            ++table_i;
        }
    }

    void
    SchemaMgr::SchemaInfo::_read_indexes_table(uint64_t table_id)
    {
        // get the "indexes" table
        auto indexes_t = TableMgr::get_instance()->get_table(sys_tbl::Indexes::ID,
                                                             constant::LATEST_XID,
                                                             constant::MAX_LSN);
        auto schema = indexes_t->extent_schema();
        auto fields = schema->get_fields();

        // read everything with the given table_id
        // note: use zeros for additional columns in the primary key to ensure we see all entries
        //       with lower_bound()
        auto search_key = sys_tbl::Indexes::Primary::key_tuple(table_id, 0, 0, 0);

        // scan for the results of the schemas_history table
        auto table_i = indexes_t->lower_bound(search_key);
        while (table_i != indexes_t->end()) {
            auto &row = *table_i;

            // get the table_id from the entry
            uint64_t tid = fields->at(sys_tbl::Indexes::Data::TABLE_ID)->get_uint64(row);
            if (tid != table_id) {
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            uint64_t index_id = fields->at(sys_tbl::Indexes::Data::INDEX_ID)->get_uint64(row);
            uint64_t xid = fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(row);
            uint32_t column_id = fields->at(sys_tbl::Indexes::Data::COLUMN_ID)->get_uint32(row);
            // note: positions should be in-order, so don't need to read them

            if (index_id == 0) {
                _primary_index[xid].push_back(column_id);
            } else {
                _secondary_indexes[index_id][xid].push_back(column_id);
            }

            ++table_i;
        }
    }

    SchemaMgr::SchemaInfo::SchemaInfo(uint64_t table_id)
    {
        _read_schema_table(table_id);
        _read_indexes_table(table_id);
    }

    std::vector<std::string>
    SchemaMgr::SchemaInfo::get_index_keys(uint64_t index_id, uint64_t xid)
    {
        std::vector<std::string> key;

        // XXX need to handle lookup failure
        auto &index = (index_id == constant::INDEX_PRIMARY)
            ? _primary_index
            : _secondary_indexes[index_id];

        auto &&i = index.lower_bound(xid);
        for (uint32_t column : i->second) {
            // XXX need to handle lookup failure
            key.push_back(_column_map[column].lower_bound(xid)->second.back().name);
        }

        return key;
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
                                              uint64_t lsn)
    {
        // determine the columns for the extent XID
        auto &&columns = _get_columns_for_xid(extent_xid);

        std::vector<SchemaColumn> updates;

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
                if (changes_i->first > target_xid) {
                    break;
                }

                // apply any updates from the xid prior to the LSN (if one was provided)
                for (const auto &update : changes_i->second) {
                    // if an LSN was provided and the update's LSN is greater than the provided LSN, stop applying updates
                    if (update.lsn > lsn) {
                        break;
                    }

                    // apply the update to the schema as a schema upgrade
                    updates.push_back(update);
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

    SchemaMgr::SchemaMgr()
        : _cache(1024*1024)
    {
        SchemaColumn child(constant::BTREE_CHILD_FIELD, 0, SchemaType::UINT64, false);

        // TableNames
        _system_cache[{ sys_tbl::TableNames::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::TableNames::Data::SCHEMA);

        auto schema = std::make_shared<ExtentSchema>(sys_tbl::TableNames::Primary::SCHEMA);
        _system_cache[{ sys_tbl::TableNames::ID, constant::INDEX_PRIMARY, true }] = schema;
        _system_cache[{ sys_tbl::TableNames::ID, constant::INDEX_PRIMARY, false }] = schema->create_schema(sys_tbl::TableNames::Primary::KEY, { child });
        
        schema = std::make_shared<ExtentSchema>(sys_tbl::TableNames::Secondary::SCHEMA);
        _system_cache[{ sys_tbl::TableNames::ID, 1, true }] = schema;
        _system_cache[{ sys_tbl::TableNames::ID, 1, false }] = schema->create_schema(sys_tbl::TableNames::Secondary::KEY, { child });

        // TableRoots
        _system_cache[{ sys_tbl::TableRoots::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::TableRoots::Data::SCHEMA);

        schema = std::make_shared<ExtentSchema>(sys_tbl::TableRoots::Primary::SCHEMA);
        _system_cache[{ sys_tbl::TableRoots::ID, constant::INDEX_PRIMARY, true }] = schema;
        _system_cache[{ sys_tbl::TableRoots::ID, constant::INDEX_PRIMARY, false }] = schema->create_schema(sys_tbl::TableRoots::Primary::KEY, { child });

        // Indexes
        _system_cache[{ sys_tbl::Indexes::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::Indexes::Data::SCHEMA);

        schema = std::make_shared<ExtentSchema>(sys_tbl::Indexes::Primary::SCHEMA);
        _system_cache[{ sys_tbl::Indexes::ID, constant::INDEX_PRIMARY, true }] = schema;
        _system_cache[{ sys_tbl::Indexes::ID, constant::INDEX_PRIMARY, false }] = schema->create_schema(sys_tbl::Indexes::Primary::KEY, { child });

        // Schemas
        _system_cache[{ sys_tbl::Schemas::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::Schemas::Data::SCHEMA);

        schema = std::make_shared<ExtentSchema>(sys_tbl::Schemas::Primary::SCHEMA);
        _system_cache[{ sys_tbl::Schemas::ID, constant::INDEX_PRIMARY, true }] = schema;
        _system_cache[{ sys_tbl::Schemas::ID, constant::INDEX_PRIMARY, false }] = schema->create_schema(sys_tbl::Schemas::Primary::KEY, { child });
        
    }

    std::map<uint32_t, SchemaColumn>
    SchemaMgr::get_columns(uint64_t table_id, uint64_t xid, uint64_t lsn)
    {
        // XXX can't call this for system tables
        assert(_system_cache.find({ table_id, constant::INDEX_DATA, true }) == _system_cache.end());

        std::shared_ptr<SchemaInfo> info = _cache.get(table_id);
        if (info == nullptr) {
            info = std::make_shared<SchemaInfo>(table_id);
            _cache.insert(table_id, info);
        }

        return info->get_columns_for_xid_lsn(xid, lsn);
    }

    std::shared_ptr<Schema>
    SchemaMgr::get_schema(uint64_t table_id, uint64_t extent_xid, uint64_t target_xid, uint64_t lsn)
    {
        // first check if it's an immutable system table schema
        auto &&system_i = _system_cache.find({ table_id, constant::INDEX_DATA, true });
        if (system_i != _system_cache.end()) {
            return system_i->second;
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

    std::shared_ptr<ExtentSchema>
    SchemaMgr::get_extent_schema(uint64_t table_id,
                                 uint64_t xid)
    {
        // first check if it's an immutable system table schema
        auto &&system_i = _system_cache.find({ table_id, constant::INDEX_DATA, true });
        if (system_i != _system_cache.end()) {
            return system_i->second;
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

    std::shared_ptr<ExtentSchema>
    SchemaMgr::get_extent_schema(uint64_t table_id,
                                 uint64_t xid,
                                 uint64_t index_id,
                                 ExtentType type)
    {
        ExtentSchemaPtr data_schema;

        // first check if it's an immutable system table schema
        auto &&system_i = _system_cache.find({ table_id, index_id, type.is_leaf() });
        if (system_i != _system_cache.end()) {
            return system_i->second;
        }

        // next check the cache
        std::shared_ptr<SchemaInfo> info = _cache.get(table_id);

        // if not in the cache then read it and insert it into the cache
        if (info == nullptr) {
            info = std::make_shared<SchemaInfo>(table_id);
            _cache.insert(table_id, info);
        }

        // retrieve the schema at the appropriate point-in-time
        auto schema = info->get_extent_schema(xid);

        // check if this is for a data extent
        if (index_id == constant::INDEX_DATA) {
            return schema;
        }

        // get the keys of the index
        auto keys = info->get_index_keys(index_id, xid);

        if (type.is_leaf()) {
            // construct a schema for the leaf extents of the index
            if (index_id == constant::INDEX_PRIMARY) {
                // primary index value column
                SchemaColumn extent_c(constant::INDEX_EID_FIELD, 0, SchemaType::UINT64, false);
                return schema->create_schema(keys, { extent_c });
            } else {
                // secondary index value columns
                SchemaColumn extent_c(constant::INDEX_EID_FIELD, 0, SchemaType::UINT64, false);
                SchemaColumn row_c(constant::INDEX_RID_FIELD, 0, SchemaType::UINT32, false);
                return schema->create_schema(keys, { extent_c, row_c });
            }
        } else {
            // branch schema value column
            SchemaColumn child(constant::BTREE_CHILD_FIELD, 0, SchemaType::UINT64, false);
            return schema->create_schema(keys, { child });
        }
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
            for (auto &&old_entry : old_schema) {
                auto &&new_i = new_schema.find(old_entry.first);
                if (new_i == new_schema.end()) {
                    // generate a REMOVE_COLUMN update
                    update.update_type = SchemaUpdateType::REMOVE_COLUMN;
                    update.position = old_entry.second.position;

                    return update;
                }
            }
        }

        // if the old schema has fewer columns, then a column was added
        if (old_schema.size() < new_schema.size()) {
            // find the missing column
            for (auto &&new_entry : new_schema) {
                auto &&old_i = old_schema.find(new_entry.first);
                if (old_i == old_schema.end()) {
                    // generate an ADD_COLUMN update
                    update.update_type = SchemaUpdateType::NEW_COLUMN;
                    update.position = new_entry.second.position;
                    update.name = new_entry.second.name;
                    update.type = new_entry.second.type;
                    update.nullable = new_entry.second.nullable;
                    update.default_value = new_entry.second.default_value;

                    return update;
                }
            }
        }

        // otherwise, compare each column to find the one with the difference
        for (auto &&old_i : old_schema) {
            // find the same column in the new schema
            auto &&new_i = new_schema.find(old_i.first);

            // it must exist or else the new and old schema are more than one modification apart
            assert(new_i != new_schema.end());

            // check for differences
            if (old_i.second.name != new_i->second.name) {
                // generate a NAME_CHANGE update
                update.update_type = SchemaUpdateType::NAME_CHANGE;
                update.position = new_i->second.position;
                update.name = new_i->second.name;

                return update;
            }

            if (old_i.second.nullable != new_i->second.nullable) {
                // generate a NULLABLE_CHANGE update
                update.update_type = SchemaUpdateType::NULLABLE_CHANGE;
                update.position = new_i->second.position;
                update.nullable = new_i->second.nullable;

                return update;
            }

            if (old_i.second.type != new_i->second.type) {
                // generate a TYPE_CHANGE update
                update.update_type = SchemaUpdateType::TYPE_CHANGE;
                update.position = new_i->second.position;
                update.type = new_i->second.type;

                return update;
            }
        }

        // XXX can there be a schema change that results in no changes on the Springtail side?
        assert(false);
    }
}
