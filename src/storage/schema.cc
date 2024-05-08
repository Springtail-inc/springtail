#include <string>

#include <fmt/core.h>

#include <storage/exception.hh>
#include <storage/schema.hh>
#include <storage/field.hh>

namespace springtail {

    void
    ExtentSchema::_populate(const std::map<uint32_t, SchemaColumn> columns)
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

            // set the column order
            _column_order.push_back(column.name);

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

                if (column.nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                }

                byte_pos += 8; // add the used bytes
                break;

            case (SchemaType::TEXT):
            case (SchemaType::BINARY):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::FLOAT32):
                field = std::make_shared<ExtentField>(column.type, byte_pos);

                if (column.nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                }
                    
                byte_pos += 4; // add the used bytes
                break;

            case (SchemaType::UINT16):
            case (SchemaType::INT16):
                field = std::make_shared<ExtentField>(column.type, byte_pos);

                if (column.nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                }

                byte_pos += 2; // add the used bytes
                break;

            case (SchemaType::UINT8):
            case (SchemaType::INT8):
                field = std::make_shared<ExtentField>(column.type, byte_pos);

                if (column.nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                }

                byte_pos += 1; // add the used bytes
                break;

            case (SchemaType::BOOLEAN): {
                // save the bool position, and count it's bit
                uint64_t bool_pos = bit_pos++;

                field = std::make_shared<ExtentField>(column.type, (bool_pos >> 3), (bool_pos & 0x7));

                if (column.nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                }
                break;
            }

            default:
                throw TypeError();
            }

            // store the field into the base map
            _field_map[column.name] = { field, idx };

            // handle primary key data
            if (column.pkey_position) {
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
                                const std::vector<std::string> &sort_columns) const
    {
        // create SchemaColumn entries for the existing fields
        std::vector<SchemaColumn> all_columns;
        for (auto &&name : old_columns) {
            auto &&i = _field_map.find(name);

            uint32_t size = all_columns.size();

            auto pos = std::ranges::find(sort_columns, name);
            if (pos == sort_columns.end()) {
                all_columns.push_back({
                        name,
                        size,
                        i->second.first->get_type(),
                        i->second.first->can_be_null()
                    });
            } else {
                all_columns.push_back({
                        name,
                        size,
                        i->second.first->get_type(),
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
        return std::make_shared<ExtentSchema>(all_columns);
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

        // find the correct column indexes for the columns
        auto fields = std::make_shared<std::vector<std::shared_ptr<Field>>>();
        for (auto &&column : columns) {
            auto &&i = _field_map.find(column);
            fields->push_back(tuple->field(i->second.second));
        }

        return std::make_shared<FieldTuple>(fields, tuple->row());
    }

    std::shared_ptr<Field>
    VirtualSchema::_make_const(SchemaType type,
                               const std::string &value)
    {
        switch(type) {
        case(SchemaType::TEXT):
            return std::make_shared<ConstTypeField<std::string>>(value);

        case(SchemaType::UINT64):
            return std::make_shared<ConstTypeField<uint64_t>>(std::stoull(value, nullptr, 0));

        case(SchemaType::INT64):
            return std::make_shared<ConstTypeField<int64_t>>(std::stoll(value, nullptr, 0));

        case(SchemaType::UINT32):
            return std::make_shared<ConstTypeField<uint32_t>>(std::stoul(value, nullptr, 0));

        case(SchemaType::INT32):
            return std::make_shared<ConstTypeField<int32_t>>(std::stol(value, nullptr, 0));

        case(SchemaType::UINT16):
            return std::make_shared<ConstTypeField<uint16_t>>(static_cast<uint16_t>(std::stoul(value, nullptr, 0)));

        case(SchemaType::INT16):
            return std::make_shared<ConstTypeField<int16_t>>(static_cast<uint16_t>(std::stoi(value, nullptr, 0)));

        case(SchemaType::UINT8):
            return std::make_shared<ConstTypeField<uint8_t>>(static_cast<uint8_t>(std::stoul(value, nullptr, 0)));

        case(SchemaType::INT8):
            return std::make_shared<ConstTypeField<int8_t>>(static_cast<int8_t>(std::stoi(value, nullptr, 0)));

        case(SchemaType::BOOLEAN):
            // note: might need a more robust way to check the default value
            return std::make_shared<ConstTypeField<bool>>(value == "true");

        case(SchemaType::FLOAT64):
            return std::make_shared<ConstTypeField<double>>(std::stod(value));

        case(SchemaType::FLOAT32):
            return std::make_shared<ConstTypeField<float>>(std::stof(value));

        case(SchemaType::BINARY):
            // note: this will currently cause two copies of the default value
            return std::make_shared<ConstTypeField<std::vector<char>>>(std::vector<char>(value.begin(), value.end()));

        default:
            throw SchemaError(fmt::format("Unsupported SchemaType: {}", static_cast<uint8_t>(type)));
        }
    }

    std::shared_ptr<Field>
    VirtualSchema::_make_default_value(std::shared_ptr<Field> field,
                                       const std::string &fallback)
    {
        switch(field->get_type()) {
        case(SchemaType::TEXT):
            return std::make_shared<DefaultValueField<std::string>>(field, fallback);

        case(SchemaType::UINT64):
            return std::make_shared<DefaultValueField<uint64_t>>(field, std::stoull(fallback, nullptr, 0));

        case(SchemaType::INT64):
            return std::make_shared<DefaultValueField<int64_t>>(field, std::stoll(fallback, nullptr, 0));

        case(SchemaType::UINT32):
            return std::make_shared<DefaultValueField<uint32_t>>(field, std::stoul(fallback, nullptr, 0));

        case(SchemaType::INT32):
            return std::make_shared<DefaultValueField<int32_t>>(field, std::stol(fallback, nullptr, 0));

        case(SchemaType::UINT16):
            return std::make_shared<DefaultValueField<uint16_t>>(field, static_cast<uint16_t>(std::stoul(fallback, nullptr, 0)));

        case(SchemaType::INT16):
            return std::make_shared<DefaultValueField<int16_t>>(field, static_cast<uint16_t>(std::stoi(fallback, nullptr, 0)));
        case(SchemaType::UINT8):
            return std::make_shared<DefaultValueField<uint8_t>>(field, static_cast<uint8_t>(std::stoul(fallback, nullptr, 0)));
        case(SchemaType::INT8):
            return std::make_shared<DefaultValueField<int8_t>>(field, static_cast<int8_t>(std::stoi(fallback, nullptr, 0)));

        case(SchemaType::BOOLEAN):
            // note: might need a more robust way to check the default value
            return std::make_shared<DefaultValueField<bool>>(field, fallback == "true");

        case(SchemaType::FLOAT64):
            return std::make_shared<DefaultValueField<double>>(field, std::stod(fallback));

        case(SchemaType::FLOAT32):
            return std::make_shared<DefaultValueField<float>>(field, std::stof(fallback));

        case(SchemaType::BINARY):
            return std::make_shared<DefaultValueField<std::vector<char>>>(field, std::vector<char>(fallback.begin(), fallback.end()));

        default:
            throw SchemaError(fmt::format("Unsupported SchemaType: {}", static_cast<uint8_t>(field->get_type())));
        }
    }

    VirtualSchema::VirtualSchema(std::shared_ptr<ExtentSchema> extent_schema,
                                 const std::map<uint32_t, SchemaColumn> &columns,
                                 const std::vector<SchemaColumn> &updates)
        : _extent_schema(extent_schema)
    {
        std::map<uint32_t, std::string> name_map;

        // get a copy of the fields from the extent schema
        for (auto &&column : columns) {
            _field_map[column.second.name] = extent_schema->get_field(column.second.name);
            name_map[column.second.position] = column.second.name;
        }

        // loop through each update and apply it to the field map
        for (auto &&update : updates) {
            // apply the change to the field set based on the kind of update being performed
            switch(update.update_type) {
            case (SchemaUpdateType::NEW_COLUMN):
                if (update.default_value) {
                    _field_map[update.name] = _make_const(update.type, *(update.default_value));
                } else {
                    _field_map[update.name] = std::make_shared<ConstNullField>(update.type);
                }

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
                if (!update.nullable) {
                    auto old_field = _field_map[update.name];
                    _field_map[update.name] = _make_default_value(old_field, *(update.default_value));
                }
                break;
            case (SchemaUpdateType::TYPE_CHANGE):
                throw SchemaError("Can't handle column type changes yet");
            }
        }
    }

    std::shared_ptr<std::vector<FieldPtr>>
    VirtualSchema::get_fields() const
    {
        return this->get_fields(_extent_schema->column_order());
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
