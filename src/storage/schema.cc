#include <fmt/core.h>

#include <storage/exception.hh>
#include <storage/schema.hh>
#include <storage/field.hh>

namespace springtail {

    static const std::vector<SchemaColumn> SchemaManager::TABLES_SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "name", 1, SchemaType::TEXT, false }
    };

    static const std::vector<SchemaColumn> SchemaManager::TABLES_SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false }, 
        { "start_xid", 1, SchemaType::UINT64, false },
        { "end_xid", 2, SchemaType::UINT64, false },  
        { "name", 3, SchemaType::TEXT, false },       
        { "position", 4, SchemaType::UINT32, false }, 
        { "type", 5, SchemaType::UINT8, false },      
        { "nullable", 6, SchemaType::BOOLEAN, false },
        { "default_value", 7, SchemaType::TEXT, true }
    };

    static const std::vector<SchemaColumn> SchemaManager::SCHEMAS_HISTORY_SCHEMA = {
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
            if (i.second.start_xid > xid) {
                continue;
            }

            // we found a valid entry, add it to the vector of columns
            columns[pair.first] = i.second;
        }

        return columns;
    }

    void
    SchemaManager::SchemaInfo::_read_schema_table(uint64_t table_id)
    {
        // first get the snapshots from the schemas table
        std::shared_ptr<Table> schemas_table = TableManager::get_table(SCHEMAS_TID);

        // read everything with the given table_id
        auto cond = std::make_unique<BinaryCondition>(std::make_unique<FieldCondition>("table_id"),
                                                      "==",
                                                      std::make_unique<ConstCondition<uint64_t>(table_id));

        auto ordering = std::make_unique<Ordering>({
                { std::make_unique<FieldCondition>("position"), Ordering::ASC },
                { std::make_unique<FieldCondition>("start_xid"), Ordering::ASC }
            });

        // construct the column accessors for the schemas table
        std::shared_ptr<Schema> schemas_s = schemas_table->get_schema();
        std::shared_ptr<Field> start_xid_f = schemas_s->get_field("start_xid");
        std::shared_ptr<Field> end_xid_f = schemas_s->get_field("end_xid");
        std::shared_ptr<Field> name_f = schemas_s->get_field("name");
        std::shared_ptr<Field> position_f = schemas_s->get_field("position");
        std::shared_ptr<Field> type_f = schemas_s->get_field("type");
        std::shared_ptr<Field> nullable_f = schemas_s->get_field("nullable");
        std::shared_ptr<Field> default_value_f = schemas_s->get_field("default_value");

        // scan for the results of the schemas table
        auto results = schemas_table->scan(cond, ordering);
        for (auto &&row : results) {
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
        std::shared_ptr<Field> xid_f = history_s->get_field("xid");
        std::shared_ptr<Field> lsn_f = history_s->get_field("lsn");
        std::shared_ptr<Field> update_type_f = history_s->get_field("update_type");
        std::shared_ptr<Field> name_f = history_s->get_field("name");
        std::shared_ptr<Field> position_f = history_s->get_field("position");
        std::shared_ptr<Field> type_f = history_s->get_field("type");
        std::shared_ptr<Field> nullable_f = history_s->get_field("nullable");
        std::shared_ptr<Field> default_value_f = history_s->get_field("default_value");

        // read everything with the given table_id
        auto cond = std::make_unique<BinaryCondition>(std::make_unique<FieldCondition>("table_id"),
                                                      "==",
                                                      std::make_unique<ConstCondition<uint64_t>(table_id));

        auto ordering = std::make_unique<Ordering>({
                { std::make_unique<FieldCondition>("posiiton"), Ordering::ASC },
                { std::make_unique<FieldCondition>("xid"), Ordering::ASC }
                { std::make_unique<FieldCondition>("lsn"), Ordering::ASC }
            });

        // scan for the results of the schemas_history table
        auto results = schemas_history->scan(cond, ordering);
        for (auto &&row : results) {
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

    SchemaManager::SchemaInfo::SchemaInfo(uint64_t table_id)
    {
        _read_schema_table(table_id);
        _read_schema_history_table(table_id);
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

    void
    ExtentSchema::_populate(const std::map<uint32_t, SchemaColumn> columns)
    {
        // first calculate the row size of the non-bitwise data
        uint32_t fixed_bytes = 0;
        for (auto &&pair : columns) {
            SchemaColumn &&column = pair.second;

            switch(column.type) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::FLOAT64):
                fixed_bytes += 8;
            break;

            case (SchemaType::TEXT):
            case (SchemaType::BINARY):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::FLOAT32):
                fixed_bytes += 4;
            break;

            case (SchemaType::UINT16):
            case (SchemaType::INT16):
                fixed_bytes += 2;
            break;

            case (SchemaType::UINT8):
            case (SchemaType::INT8):
                fixed_bytes += 1;
            break;

            case (SchemaType::BOOLEAN):
                // only uses a bit, no full bytes
                break;

            default:
                throw TypeError();
            }
        }

        // then construct the fields
        uint32_t byte_pos = 0;
        uint64_t bit_pos = static_cast<uint64_t>(fixed_bytes) << 3;

        for (auto &&pair : columns) {
            SchemaColumn &&column = pair.second;

            // construct based on the column type
            std::shared_ptr<Field> field;
            switch (column.type) {
            case (SchemaType::TEXT):
                if (column.nullable) {
                    field = std::make_shared<NullableTextField>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<TextField>(byte_pos);
                }
                    
                byte_pos += 4; // add the used bytes
                break;
            case (SchemaType::BINARY):
                if (column.nullable) {
                    field = std::make_shared<NullableBinaryField>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<BinaryField>(byte_pos);
                }

                byte_pos += 4; // add the used bytes
                break;
            case (SchemaType::UINT64):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<uint64_t>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<uint64_t>>(byte_pos);
                }

                byte_pos += 8; // add the used bytes
                break;
            case (SchemaType::INT64):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<int64_t>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<int64_t>>(byte_pos);
                }

                byte_pos += 8; // add the used bytes
                break;
            case (SchemaType::UINT32):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<uint32_t>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<uint32_t>>(byte_pos);
                }

                byte_pos += 4; // add the used bytes
                break;
            case (SchemaType::INT32):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<int32_t>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<int32_t>>(byte_pos);
                }

                byte_pos += 4; // add the used bytes
                break;
            case (SchemaType::UINT16):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<uint16_t>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<uint16_t>>(byte_pos);
                }

                byte_pos += 2; // add the used bytes
                break;
            case (SchemaType::INT16):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<int16_t>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<int16_t>>(byte_pos);
                }

                byte_pos += 2; // add the used bytes
                break;
            case (SchemaType::UINT8):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<uint8_t>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<uint8_t>>(byte_pos);
                }

                byte_pos += 1; // add the used bytes
                break;
            case (SchemaType::INT8):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<int8_t>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<int8_t>>(byte_pos);
                }

                byte_pos += 1; // add the used bytes
                break;
            case (SchemaType::BOOLEAN):
                if (column.nullable) {
                    // save the bool position, and count it's bit
                    uint64_t bool_pos = bit_pos++;

                    field = std::make_shared<NullableBooleanField>((bool_pos >> 3), (bool_pos & 0x7), (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<BooleanField>(byte_pos);
                }
                break;
            case (SchemaType::FLOAT64):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<double>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<double>>(byte_pos);
                }

                byte_pos += 1; // add the used bytes
                break;

            case (SchemaType::FLOAT32):
                if (column.nullable) {
                    field = std::make_shared<NullableNumberField<float>>(byte_pos, (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    field = std::make_shared<NumberField<float>>(byte_pos);
                }

                byte_pos += 1; // add the used bytes
                break;

            default:
                throw TypeError();
            }

            // store the field into the base map
            _field_map[column.name] = field;
        }

        // save the total row size
        _row_size = (bit_pos + 7) >> 3;
    }

    std::shared_ptr<Field>
    VirtualSchema::_make_const(SchemaType type,
                               const std::string &value)
    {
        switch(type) {
        case(SchemaType::TEXT):
            return std::make_shared<ConstField<std::string>>(value);

        case(SchemaType::UINT64):
            return std::make_shared<ConstField<uint64_t>>(std::stoull(value, nullptr, 0));

        case(SchemaType::INT64):
            return std::make_shared<ConstField<int64_t>>(std::stoll(value, nullptr, 0));

        case(SchemaType::UINT32):
            return std::make_shared<ConstField<uint32_t>>(std::stoul(value, nullptr, 0));

        case(SchemaType::INT32):
            return std::make_shared<ConstField<int32_t>>(std::stol(value, nullptr, 0));

        case(SchemaType::UINT16):
            return std::make_shared<ConstField<uint16_t>>(static_cast<uint16_t>(std::stoui(value, nullptr, 0)));

        case(SchemaType::INT16):
            return std::make_shared<ConstField<int16_t>>(static_cast<uint16_t>(std::stoi(value, nullptr, 0)));
        case(SchemaType::UINT8):
            return std::make_shared<ConstField<uint8_t>>(static_cast<uint8_t>(std::stoui(value, nullptr, 0)));
        case(SchemaType::INT8):
            return std::make_shared<ConstField<int8_t>>(static_cast<int8_t>(std::stoi(value, nullptr, 0)));

        case(SchemaType::BOOLEAN):
            // note: might need a more robust way to check the default value
            return std::make_shared<ConstField<bool>>(value == "true");

        case(SchemaType::FLOAT64):
            return std::make_shared<ConstField<double>>(std::stod(value, nullptr, 0));

        case(SchemaType::FLOAT32):
            return std::make_shared<ConstField<float>>(std::stof(value, nullptr, 0));

        case(SchemaType::BINARY):
            return std::make_shared<ConstField<std::vector<char>>>(value.begin(), value.end());

        default:
            throw SchemaError(fmt::format("Unsupported SchemaType: {}", type));
        }
    }

    std::shared_ptr<Field>
    VirtualSchema::_make_null_wrapper(std::shared_ptr<Field> field,
                                      const std::string &fallback)
    {
        switch(field->get_type()) {
        case(SchemaType::TEXT):
            return std::make_shared<NullWrapperField<std::string>>(field, fallback);

        case(SchemaType::UINT64):
            return std::make_shared<NullWrapperField<uint64_t>>(field, std::stoull(fallback, nullptr, 0));

        case(SchemaType::INT64):
            return std::make_shared<NullWrapperField<int64_t>>(field, std::stoll(fallback, nullptr, 0));

        case(SchemaType::UINT32):
            return std::make_shared<NullWrapperField<uint32_t>>(field, std::stoul(fallback, nullptr, 0));

        case(SchemaType::INT32):
            return std::make_shared<NullWrapperField<int32_t>>(field, std::stol(fallback, nullptr, 0));

        case(SchemaType::UINT16):
            return std::make_shared<NullWrapperField<uint16_t>>(field, static_cast<uint16_t>(std::stoui(fallback, nullptr, 0)));

        case(SchemaType::INT16):
            return std::make_shared<NullWrapperField<int16_t>>(field, static_cast<uint16_t>(std::stoi(fallback, nullptr, 0)));
        case(SchemaType::UINT8):
            return std::make_shared<NullWrapperField<uint8_t>>(field, static_cast<uint8_t>(std::stoui(fallback, nullptr, 0)));
        case(SchemaType::INT8):
            return std::make_shared<NullWrapperField<int8_t>>(field, static_cast<int8_t>(std::stoi(fallback, nullptr, 0)));

        case(SchemaType::BOOLEAN):
            // note: might need a more robust way to check the default value
            return std::make_shared<NullWrapperField<bool>>(field, fallback == "true");

        case(SchemaType::FLOAT64):
            return std::make_shared<NullWrapperField<double>>(field, std::stod(fallback, nullptr, 0));

        case(SchemaType::FLOAT32):
            return std::make_shared<NullWrapperField<float>>(field, std::stof(fallback, nullptr, 0));

        case(SchemaType::BINARY):
            return std::make_shared<NullWrapperField<std::vector<char>>>(field, std::vector<char>(fallback.begin(), fallback.end()));

        default:
            throw SchemaError(fmt::format("Unsupported SchemaType: {}", type));
        }
    }

    VirtualSchema::VirtualSchema(std::shared_ptr<ExtentSchema> extent_schema,
                                 const std::map<uint32_t, SchemaColumn> &columns,
                                 const std::vector<SchemaUpdate> &updates)
    {
        // get a copy of the fields from the extent schema
        for (auto &&column : columns) {
            _field_map[column.second.name] = extent_schema->get_field(column.second.name);
        }

        // loop through each update and apply it to the field map
        for (auto &&update : updates) {
            // apply the change to the field set based on the kind of update being performed
            switch(update.type) {
            case (SchemaUpdateType::NEW_COLUMN):
                if (update.default_value) {
                    _field_map[update.name] = _make_const(update.type, update.default_value);
                } else {
                    _field_map[update.name] = std::make_shared<ConstNullField>(update.type);
                }

                columns[update.position] = SchemaColumn(update);
                break;
            case (SchemaUpdateType::REMOVE_COLUMN):
                _field_map.erase(update.name);

                columns.erase(update.position);
                break;
            case (SchemaUpdateType::NAME_CHANGE):
                auto &&i = _field_map.find(columns[update.position].name);
                auto field = i.second;
                _field_map.erase(i);
                _field_map[update.name] = field;

                columns[update.position].name = update.name;
                break;
            case (SchemaUpdateType::NULLABLE_CHANGE):
                if (!update.nullable) {
                    auto old_field = _field_map[update.name];
                    _field_map[update.name] = _make_null_wrapper(old_field, update.default_value);
                }

                columns[update.position].nullable = update.nullable;
                break;
            case (SchemaUpdateType::TYPE_CHANGE):
                throw SchemaError("Can't handle column type changes yet");
            }
        }
    }
}
