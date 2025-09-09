#include <storage/schema.hh>
#include <storage/field.hh>

namespace springtail {

    void
    ExtentSchema::_populate(const std::map<uint32_t, SchemaColumn>& columns,
                            bool allow_undefined,
                            ComparatorFunc comparator_func)
    {
        // track how many primary key columns there are
        uint32_t pkey_count = 0;

        // first calculate the row size of the non-bitwise data
        uint32_t fixed_bytes = 0;
        for (auto &&pair : columns) {
            const SchemaColumn &column = pair.second;

            switch(column.type) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::FLOAT64):
                fixed_bytes += 8;
            break;

            case (SchemaType::TEXT):
            case (SchemaType::BINARY):
            case (SchemaType::NUMERIC):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::FLOAT32):
                fixed_bytes += 4;
            break;

            case (SchemaType::EXTENSION):
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
                LOG_ERROR("Unsupported column type: {}", (int)column.type);
                throw TypeError();
            }

            // set the column order
            _column_order.push_back(column.name);

            // add the sql types in column order
            _pg_types.push_back(column.pg_type);

            // add the field types in column order
            uint8_t type = static_cast<uint8_t>(column.type);
            if (column.nullable) {
                type |= 0x80;
            }
            _field_types.push_back(type);

            // save the pkey mappings
            if (column.pkey_position) {
                ++pkey_count;
            }
        }

        // construct the sorting vectors
        _sort_fields = std::make_shared<std::vector<FieldPtr>>(pkey_count);
        _sort_keys.resize(pkey_count);

        // then construct the fields
        uint32_t byte_pos = 0;
        uint64_t bit_pos = static_cast<uint64_t>(fixed_bytes) << 3;

        int idx = 0;
        for (auto &&pair : columns) {
            const SchemaColumn &column = pair.second;

            // construct based on the column type
            std::shared_ptr<ExtentField> field;

            switch (column.type) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::FLOAT64):
                field = std::make_shared<ExtentField>(column.type, byte_pos);
                byte_pos += 8; // add the used bytes
                break;

            case (SchemaType::TEXT):
            case (SchemaType::BINARY):
            case (SchemaType::NUMERIC):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::FLOAT32):
                field = std::make_shared<ExtentField>(column.type, byte_pos);
                byte_pos += 4; // add the used bytes
                break;

            case (SchemaType::EXTENSION): {
                if ( comparator_func ) {
                    // Override the compartor function if its present
                    auto updated_comparator_func = [column, comparator_func](std::string_view op_str,
                                                                             const std::span<const char> &lhval,
                                                                             const std::span<const char> &rhval) -> bool {
                        return comparator_func(column.pg_type, op_str, lhval, rhval);
                    };
                    field = std::make_shared<ExtentField>(column.type, byte_pos, updated_comparator_func);
                } else {
                    field = std::make_shared<ExtentField>(column.type, byte_pos);
                }

                if (column.nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                }

                byte_pos += 4; // add the used bytes
                break;
            }

            case (SchemaType::UINT16):
            case (SchemaType::INT16):
                field = std::make_shared<ExtentField>(column.type, byte_pos);
                byte_pos += 2; // add the used bytes
                break;

            case (SchemaType::UINT8):
            case (SchemaType::INT8):
                field = std::make_shared<ExtentField>(column.type, byte_pos);
                byte_pos += 1; // add the used bytes
                break;

            case (SchemaType::BOOLEAN): {
                // save the bool position, and count it's bit
                uint64_t bool_pos = bit_pos++;
                field = std::make_shared<ExtentField>(column.type, (bool_pos >> 3), (bool_pos & 0x7));
                break;
            }

            default:
                throw TypeError();
            }

            // update the field to be nullable if applicable
            if (column.nullable) {
                field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                ++bit_pos; // increment for the null bit
            }

            if (allow_undefined && !column.pkey_position) {
                field->allow_undefined((bit_pos >> 3), (bit_pos & 0x7));
                ++bit_pos; // increment for the undefined bit
            }

            // store the field into the base map
            _field_map[column.name] = { field, idx };

            // handle primary key data
            if (column.pkey_position) {
                assert(_sort_keys[*column.pkey_position].empty());
                _sort_fields->at(*column.pkey_position) = field;
                _sort_keys[*column.pkey_position] = column.name;
            }

            ++idx;
        }

        // save the total row size
        _row_size = (bit_pos + 7) >> 3;
    }

    std::shared_ptr<Field>
    ExtentSchema::get_field(const std::string &name) const
    {
        return get_mutable_field(name);
    }

    std::shared_ptr<ExtentSchema>
    ExtentSchema::create_schema(const std::vector<std::string> &old_columns,
                                const std::vector<SchemaColumn> &new_columns,
                                const std::vector<std::string> &sort_columns,
                                bool allow_undefined) const
    {
        return create_schema(old_columns, new_columns, sort_columns, nullptr);
    }

    std::shared_ptr<ExtentSchema>
    ExtentSchema::create_schema(const std::vector<std::string> &old_columns,
                                const std::vector<SchemaColumn> &new_columns,
                                const std::vector<std::string> &sort_columns,
                                ComparatorFunc comparator_func) const
    {
        // create SchemaColumn entries for the existing fields
        std::vector<SchemaColumn> all_columns;
        for (auto &&name : old_columns) {
            auto &&i = _field_map.find(name);

            int32_t size = all_columns.size();

            auto pos = std::ranges::find(sort_columns, name);
            if (pos == sort_columns.end()) {
                all_columns.push_back({
                        name,
                        size,
                        i->second.first->get_type(),
                        _pg_types[i->second.second],
                        i->second.first->can_be_null()
                    });
            } else {
                all_columns.push_back({
                        name,
                        size,
                        i->second.first->get_type(),
                        _pg_types[i->second.second],
                        i->second.first->can_be_null(),
                        (pos - sort_columns.begin())
                    });
            }
        }

        // add in the new columns
        for (auto column : new_columns) {
            int size = all_columns.size();

            auto pos = std::ranges::find(sort_columns, column.name);
            if (pos != sort_columns.end()) {
                column.pkey_position = (pos - sort_columns.begin());
            }
            all_columns.push_back(column);
            all_columns.back().position = size;
        }

        // create the new ExtentSchema
        return std::make_shared<ExtentSchema>(all_columns, allow_undefined, comparator_func);
    }

    std::shared_ptr<std::vector<FieldPtr>>
    ExtentSchema::get_fields() const
    {
        return get_fields(_column_order);
    }

    std::shared_ptr<std::vector<MutableFieldPtr>>
    ExtentSchema::get_mutable_fields() const
    {
        return get_mutable_fields(_column_order);
    }

    std::shared_ptr<std::vector<FieldPtr>>
    ExtentSchema::get_fields(const std::vector<std::string> &columns) const
    {
        auto fields = std::make_shared<std::vector<FieldPtr>>();

        for (auto &&name : columns) {
            fields->push_back(this->get_mutable_field(name));
        }

        return fields;
    }

    std::shared_ptr<std::vector<MutableFieldPtr>>
    ExtentSchema::get_mutable_fields(const std::vector<std::string> &columns) const
    {
        auto fields = std::make_shared<std::vector<MutableFieldPtr>>();

        for (auto &&name : columns) {
            fields->push_back(this->get_mutable_field(name));
        }

        return fields;
    }

    std::shared_ptr<Tuple>
    ExtentSchema::tuple_subset(std::shared_ptr<Tuple> tuple,
                               const std::vector<std::string> &columns) const
    {
        // note: could add a debugging check here to ensure that:
        // 1) size of tuple matches the schema column count
        // 2) the types of the tuple match the schema columns

        auto fields = fieldarray_subset(tuple, columns);

        return std::make_shared<FieldTuple>(fields, tuple->row());
    }


    std::shared_ptr<std::vector<std::shared_ptr<Field>>>
    ExtentSchema::fieldarray_subset(std::shared_ptr<Tuple> tuple,
                               const std::vector<std::string> &columns) const
    {

        // find the correct column indexes for the columns
        auto fields = std::make_shared<std::vector<std::shared_ptr<Field>>>();
        for (auto &&column : columns) {
            auto &&i = _field_map.find(column);
            assert(i != _field_map.end());
            fields->push_back(tuple->field(i->second.second));
        }

        return fields;
    }

    VirtualSchema::VirtualSchema(const SchemaMetadata &meta, ComparatorFunc comparator_func)
    {
        std::map<uint32_t, std::string> name_map;

        // generate the extent schema from the base columns
        _extent_schema = std::make_shared<ExtentSchema>(meta.columns, comparator_func);

        // get a copy of the fields from the extent schema
        for (auto &column : meta.columns) {
            _field_map[column.name] = _extent_schema->get_field(column.name);
            name_map[column.position] = column.name;
        }

        // loop through each update and apply it to the field map
        for (auto &&update : meta.history) {
            // apply the change to the field set based on the kind of update being performed
            switch(update.update_type) {
            case (SchemaUpdateType::NEW_COLUMN):
                // note: if the new column was not nullable then we needed to perform a full sync to
                //       populate the column data, so we should never hit this path
                //assert(update.nullable);

                _field_map[update.name] = std::make_shared<ConstNullField>(update.type);
                name_map[update.position] = update.name;
                break;

            case (SchemaUpdateType::REMOVE_COLUMN):
                _field_map.erase(update.name);
                name_map.erase(update.position);
                break;

            case (SchemaUpdateType::NAME_CHANGE):
                {
                    auto &&i = _field_map.find(name_map[update.position]);
                    auto field = i->second;
                    _field_map.erase(i);
                    _field_map[update.name] = field;

                    name_map[update.position] = update.name;
                }
                break;

            case (SchemaUpdateType::NULLABLE_CHANGE):
                // note: if the column was made non-null with a default value, then we needed to
                //       perform a full sync to populate the column data, so we should never hit
                //       this path
                assert(update.nullable);
                break;


            default:
                // note: we shouldn't see any other update types in the schema history
                assert(0);
            }
        }

        // order the columns based on position
        for (const auto &entry : name_map) {
            _column_order.push_back(entry.second);
        }
    }

    std::shared_ptr<std::vector<FieldPtr>>
    VirtualSchema::get_fields() const
    {
        return this->get_fields(_column_order);
    }

    std::shared_ptr<std::vector<FieldPtr>>
    VirtualSchema::get_fields(const std::vector<std::string> &columns) const
    {
        auto fields = std::make_shared<std::vector<std::shared_ptr<Field>>>();

        for (auto &&name : columns) {
            fields->push_back(this->get_field(name));
        }

        return fields;
    }
}
