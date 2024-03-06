#include <storage/exception.hh>
#include <storage/schema.hh>
#include <storage/schema_manager.hh>

namespace springtail {
    const std::vector<SchemaColumn> SchemaManager::TABLES_SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "name", 1, SchemaType::TEXT, false }
    };

    const std::vector<SchemaColumn> SchemaManager::SCHEMAS_SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false }, 
        { "start_xid", 1, SchemaType::UINT64, false },
        { "end_xid", 2, SchemaType::UINT64, false },  
        { "name", 3, SchemaType::TEXT, false },       
        { "position", 4, SchemaType::UINT32, false }, 
        { "type", 5, SchemaType::UINT8, false },      
        { "nullable", 6, SchemaType::BOOLEAN, false },
        { "default_value", 7, SchemaType::TEXT, true }
    };

    const std::vector<SchemaColumn> SchemaManager::SCHEMAS_HISTORY_SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "xid", 1, SchemaType::UINT64, false },
        { "lsn", 2, SchemaType::UINT64, false },
        { "update_type", 3, SchemaType::UINT8, false },
        { "name", 3, SchemaType::TEXT, false },
        { "position", 4, SchemaType::UINT32, false },
        { "type", 5, SchemaType::UINT8, false },
        { "nullable", 6, SchemaType::BOOLEAN, false },
        { "default_value", 7, SchemaType::TEXT, true }
    };

    std::map<uint32_t, SchemaColumn>
    SchemaManager::SchemaInfo::_get_columns_for_xid(uint64_t xid)
    {
        std::map<uint32_t, SchemaColumn> columns;

        // for each column, try to find a valid SchemaColumn for the provided xid
        for (auto &&pair : _column_map) {
            // XXX double check lower_bound or upper_bound
            auto &&i = pair.second.lower_bound(xid);

            // if there's no entry for this column with end_xid > extent_xid, go to the next column
            if (i == pair.second.end()) {
                continue;
            }

            // if the start_xid is after the extent_xid then go to the next column
            if (i->second.start_xid > xid) {
                continue;
            }

            // we found a valid entry, add it to the vector of columns
            columns[pair.first] = i->second;
        }

        return columns;
    }

    void
    SchemaManager::SchemaInfo::_read_schema_table(uint64_t table_id)
    {
        // first get the snapshots from the schemas table
        std::shared_ptr<Table> schemas_table = TableManager::get_table(SCHEMAS_TID);

        // construct the column accessors for the schemas table
        std::shared_ptr<Schema> schemas_s = schemas_table->get_schema();

        std::shared_ptr<Field> table_id_f = schemas_s->get_field("table_id");
        std::shared_ptr<Field> start_xid_f = schemas_s->get_field("start_xid");
        std::shared_ptr<Field> end_xid_f = schemas_s->get_field("end_xid");
        std::shared_ptr<Field> name_f = schemas_s->get_field("name");
        std::shared_ptr<Field> position_f = schemas_s->get_field("position");
        std::shared_ptr<Field> type_f = schemas_s->get_field("type");
        std::shared_ptr<Field> nullable_f = schemas_s->get_field("nullable");
        std::shared_ptr<Field> default_value_f = schemas_s->get_field("default_value");

        // read everything with the given table_id
        // note: use null entries for additional columns in the primary key to ensure we see all entries
        auto search_key = std::make_shared<ValueTuplePtr>({
                std::make_shared<ConstField<uint64_t>>(table_id),
                std::make_shared<ConstNullField>(SchemaType::UINT32),
                std::make_shared<ConstNullField>(SchemaType::UINT64)
            });

        // scan for the results of the schemas table
        auto table_i = schemas_table->lower_bound(search_key);
        while (table_i != schemas_table->end()) {
            Extent::Row &row = *table_i;

            // get the table_id from the entry
            uint64_t tid = table_id_f->get_uint64(row);
            if (tid != table_id) {
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            // construct a SchemaColumn for each row
            SchemaColumn column;
            column.start_xid = start_xid_f->get_uint64(row);
            column.end_xid = end_xid_f->get_uint64(row);
            column.name = name_f->get_text(row);
            column.position = position_f->get_uint32(row);
            column.type = type_f->get_uint8(row);
            column.nullable = nullable_f->get_bool(row);
            if (!default_value_f->is_null(row)) {
                column.default_value = default_value_f->get_text(row);
            }

            // place the column into the map
            _column_map[column.position][column.end_xid] = column;
        }
    }

    void
    SchemaManager::SchemaInfo::_read_schema_history_table(uint64_t table_id)
    {
        // next get the version history from the schemas_history table
        std::shared_ptr<Table> schemas_history = TableManager::get_table(SCHEMAS_HISTORY_TID);

        // construct the column accessors for the schemas table
        std::shared_ptr<Schema> history_s = schemas_history->get_schema();

        std::shared_ptr<Field> table_id_f = history_s->get_field("table_id");
        std::shared_ptr<Field> xid_f = history_s->get_field("xid");
        std::shared_ptr<Field> lsn_f = history_s->get_field("lsn");
        std::shared_ptr<Field> update_type_f = history_s->get_field("update_type");
        std::shared_ptr<Field> name_f = history_s->get_field("name");
        std::shared_ptr<Field> position_f = history_s->get_field("position");
        std::shared_ptr<Field> type_f = history_s->get_field("type");
        std::shared_ptr<Field> nullable_f = history_s->get_field("nullable");
        std::shared_ptr<Field> default_value_f = history_s->get_field("default_value");

        // read everything with the given table_id
        // note: use null entries for additional columns in the primary key to ensure we see all entries
        auto search_key = std::make_shared<ValueTuplePtr>({
                std::make_shared<ConstField<uint64_t>>(table_id),
                std::make_shared<ConstNullField>(SchemaType::UINT32),
                std::make_shared<ConstNullField>(SchemaType::UINT64)
            });

        // scan for the results of the schemas_history table
        auto table_i = schemas_history->lower_bound(search_key);
        while (table_i != schemas_history->end()) {
            Extent::Row &row = *table_i;

            // get the table_id from the entry
            uint64_t tid = table_id_f->get_uint64(row);
            if (tid != table_id) {
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            // construct a SchemaUpdate for each row
            SchemaUpdate update;
            update.xid = xid_f->get_uint64(row);
            update.lsn = lsn_f->get_uint64(row);
            update.update_type = update_type_f->get_uint8(row);
            update.name = name_f->get_text(row);
            update.position = position_f->get_uint32(row);
            update.type = type_f->get_uint8(row);
            update.nullable = nullable_f->get_bool(row);
            if (!default_value_f->is_null(row)) {
                update.default_value = default_value_f->get_text(row);
            }

            // place the column into the map
            _column_updates[position_f->get_uint32()][update.xid].push_back(update);
        }
    }

    void
    SchemaManager::SchemaInfo::_read_primary_indexes_table(uint64_t table_id)
    {
        // next get the version history from the schemas_history table
        std::shared_ptr<Table> primary_indexes = TableManager::get_table(PRIMARY_INDEXES_TID);

        // construct the column accessors for the primary indexes table
        std::shared_ptr<Schema> primary_s = primary_indexes->get_schema();

        std::shared_ptr<Field> table_id_f = primary_s->get_field("table_id");
        std::shared_ptr<Field> start_xid_f = primary_s->get_field("start_xid");
        std::shared_ptr<Field> end_xid_f = primary_s->get_field("end_xid");
        std::shared_ptr<Field> position_f = primary_s->get_field("position");
        std::shared_ptr<Field> column_id_f = primary_s->get_field("column_id");

        // read everything with the given table_id
        // note: use null entries for additional columns in the primary key to ensure we see all entries
        auto search_key = std::make_shared<ValueTuplePtr>({
                std::make_shared<ConstField<uint64_t>>(table_id),
                std::make_shared<ConstNullField>(SchemaType::UINT32),
                std::make_shared<ConstNullField>(SchemaType::UINT64)
            });

        // scan for the results of the schemas_history table
        auto table_i = schemas_history->lower_bound(search_key);
        while (table_i != schemas_history->end()) {
            Extent::Row &row = *table_i;

            // get the table_id from the entry
            uint64_t tid = table_id_f->get_uint64(row);
            if (tid != table_id) {
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            // note: table is sorted by <table_id, start_xid, position>, making it safe to append
            //       columns to the vector while scanning
            _primary_index[start_xid_f->get_uint64(row)].push_back(column_id_f->get_uint32(row));
            if (position_f->get_uint32(row) == 0 && !end_xid_f->is_null(row)) {
                // populate an empty primary key at the end XID in case it was removed
                _primary_index[end_xid_f->get_uint64(row)];
            }
        }
    }

    SchemaManager::SchemaInfo::SchemaInfo(uint64_t table_id)
    {
        _read_schema_table(table_id);
        _read_schema_history_table(table_id);
        _read_primary_indexes_table(table_id);
    }

    std::vector<std::string>
    SchemaManager::SchemaInfo::get_primary_key(uint64_t xid)
    {
        std::vector<std::string> key;

        auto &&i = _primary_index.lower_bound(xid);
        for (uint32_t column : i->second) {
            // XXX need to handle lookup failure
            key.push_back(_column_map[columns].lower_bound(xid)->second.name);
        }
    }

    std::shared_ptr<ExtentSchema>
    SchemaManager::SchemaInfo::get_extent_schema(uint64_t extent_xid)
    {
        // determine the columns for the extent XID
        auto &&columns = _get_columns_for_xid(extent_xid);

        // use the vector of columns to generate the schema
        return std::make_shared<ExtentSchema>(columns);
    }

    std::shared_ptr<VirtualSchema>
    SchemaManager::SchemaInfo::get_virtual_schema(uint32_t extent_xid,
                                                  uint64_t target_xid,
                                                  uint64_t lsn=0)
    {
        // determine the columns for the extent XID
        auto &&columns = _get_columns_for_xid(extent_xid);

        // now find all of the updates for each column and apply them to the schema
        for (auto &&column : columns) {
            auto i = _column_updates.find(column.position);

            // if there are no updates for a column, move to the next one
            if (i == _column_updates.end()) {
                continue;
            }

            // find any updates from xids after the extent schema xid
            auto j = i.second.upper_bound(extent_xid);
            while (true) {
                // no such updates, so continue to the next column
                if (j == i.second.end()) {
                    break;
                }

                // if any such updates are past the target_xid, then continue to the next column
                if (j.first > target_xid) {
                    break;
                }

                // apply any updates from the xid prior to the LSN (if one was provided)
                for (auto &&update : j.second) {
                    // if an LSN was provided and the update's LSN is greater than the provided LSN, stop applying updates
                    if (lsn && update.lsn > lsn) {
                        break;
                    }

                    // apply the update to the schema as a schema upgrade
                    schema->apply_update(update);
                }

                // move to the next xid with schema updates
                ++j;
            }
        }

        // first create the extent schema
        auto schema = std::make_shared<ExtentSchema>(columns);

        // then layer the virtual schema
        return std::make_shared<VirtualSchema>(schema, columns, updates);
    }

    SchemaManager::SchemaManager() {
        _system_cache[TABLES_TID] = std::make_shared<ExtentSchema>(TABLES_SCHEMA);
        _system_cache[SCHEMAS_TID] = std::make_shared<ExtentSchema>(SCHEMAS_SCHEMA);
        _system_cache[SCHEMAS_HISTORY_TID] = std::make_shared<ExtentSchema>(SCHEMAS_SCHEMA);
    }

    std::shared_ptr<const Schema>
    SchemaManager::get_schema(uint64_t table_id, uint64_t extent_xid, uint64_t target_xid, uint64_t lsn = 0)
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
        return info->get_virtual_schema(extent_xid, target_xid, lsn);
    }

    std::shared_ptr<const ExtentSchema>
    SchemaManager::get_extent_schema(uint64_t table_id,
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

    SchemaUpdate
    SchemaManager::generate_update(const std::map<uint32_t, SchemaColumn> &old_schema,
                                   const std::map<uint32_t, SchemaColumn> &new_schema,
                                   uint64_t xid,
                                   uint64_t lsn)
    {
        SchemaUpdate update;
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
