#include <string>

#include <fmt/core.h>

#include <storage/exception.hh>
#include <storage/schema.hh>
#include <storage/field.hh>

namespace springtail {

    void
    ExtentSchema::_populate(const std::map<uint32_t, SchemaColumn> columns)
    {
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
        }

        // then construct the fields
        uint32_t byte_pos = 0;
        uint64_t bit_pos = static_cast<uint64_t>(fixed_bytes) << 3;

        int idx = 0;
        for (auto &&pair : columns) {
            const SchemaColumn &column = pair.second;

            // construct based on the column type
            std::shared_ptr<MutableField> field;
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

                    field = std::make_shared<NullableBoolField>((bool_pos >> 3), (bool_pos & 0x7), (bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos; // add the used null bit
                } else {
                    // save the bool position, and count it's bit
                    uint64_t bool_pos = bit_pos++;

                    field = std::make_shared<BoolField>((bool_pos >> 3), (bool_pos & 0x7));
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
            _field_map[column.name] = { field, idx };
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
    ExtentSchema::create_schema(const std::vector<std::string> &columns,
                                const std::vector<SchemaColumn> &new_columns) const
    {
        // create SchemaColumn entries for the existing fields
        std::vector<SchemaColumn> all_columns;
        for (auto &&column : columns) {
            auto &&i = _field_map.find(column);

            uint32_t size = all_columns.size();
            all_columns.push_back({
                    column,
                    size,
                    i->second.first->get_type(),
                    i->second.first->is_nullable()
                });
        }

        // add in the new columns
        for (auto &&column : new_columns) {
            int size = all_columns.size();
            all_columns.push_back(column);
            all_columns.back().position = size;
        }

        // create the new ExtentSchema
        return std::make_shared<ExtentSchema>(all_columns);
    }

    FieldArrayPtr
    ExtentSchema::get_fields() const
    {
        return get_mutable_fields();
    }

    MutableFieldArrayPtr
    ExtentSchema::get_mutable_fields() const
    {
        return get_mutable_fields(_column_order);
    }

    FieldArrayPtr
    ExtentSchema::get_fields(const std::vector<std::string> &columns) const
    {
        return get_mutable_fields(columns);
    }

    MutableFieldArrayPtr
    ExtentSchema::get_mutable_fields(const std::vector<std::string> &columns) const
    {
        std::vector<MutableFieldPtr> fields;

        for (auto &&name : columns) {
            fields.push_back(this->get_mutable_field(name));
        }

        return std::make_shared<MutableFieldArray>(fields);
    }

    std::shared_ptr<Tuple>
    ExtentSchema::tuple_subset(std::shared_ptr<Tuple> tuple,
                               const std::vector<std::string> &columns) const
    {
        // note: could add a debugging check here to ensure that:
        // 1) size of tuple matches the schema column count
        // 2) the types of the tuple match the schema columns

        // find the correct column indexes for the columns
        std::vector<std::shared_ptr<Field>> fields;
        for (auto &&column : columns) {
            auto &&i = _field_map.find(column);
            fields.push_back(tuple->field(i->second.second));
        }

        auto array = std::make_shared<ReadFieldArray>(fields);
        return array->bind(tuple->row());
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
            return std::make_shared<ConstField<uint16_t>>(static_cast<uint16_t>(std::stoul(value, nullptr, 0)));

        case(SchemaType::INT16):
            return std::make_shared<ConstField<int16_t>>(static_cast<uint16_t>(std::stoi(value, nullptr, 0)));
        case(SchemaType::UINT8):
            return std::make_shared<ConstField<uint8_t>>(static_cast<uint8_t>(std::stoul(value, nullptr, 0)));
        case(SchemaType::INT8):
            return std::make_shared<ConstField<int8_t>>(static_cast<int8_t>(std::stoi(value, nullptr, 0)));

        case(SchemaType::BOOLEAN):
            // note: might need a more robust way to check the default value
            return std::make_shared<ConstField<bool>>(value == "true");

        case(SchemaType::FLOAT64):
            return std::make_shared<ConstField<double>>(std::stod(value));

        case(SchemaType::FLOAT32):
            return std::make_shared<ConstField<float>>(std::stof(value));

        case(SchemaType::BINARY):
            // note: this will currently cause two copies of the default value
            return std::make_shared<ConstField<std::vector<char>>>(std::vector<char>(value.begin(), value.end()));

        default:
            throw SchemaError(fmt::format("Unsupported SchemaType: {}", static_cast<uint8_t>(type)));
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
            return std::make_shared<NullWrapperField<uint16_t>>(field, static_cast<uint16_t>(std::stoul(fallback, nullptr, 0)));

        case(SchemaType::INT16):
            return std::make_shared<NullWrapperField<int16_t>>(field, static_cast<uint16_t>(std::stoi(fallback, nullptr, 0)));
        case(SchemaType::UINT8):
            return std::make_shared<NullWrapperField<uint8_t>>(field, static_cast<uint8_t>(std::stoul(fallback, nullptr, 0)));
        case(SchemaType::INT8):
            return std::make_shared<NullWrapperField<int8_t>>(field, static_cast<int8_t>(std::stoi(fallback, nullptr, 0)));

        case(SchemaType::BOOLEAN):
            // note: might need a more robust way to check the default value
            return std::make_shared<NullWrapperField<bool>>(field, fallback == "true");

        case(SchemaType::FLOAT64):
            return std::make_shared<NullWrapperField<double>>(field, std::stod(fallback));

        case(SchemaType::FLOAT32):
            return std::make_shared<NullWrapperField<float>>(field, std::stof(fallback));

        case(SchemaType::BINARY):
            return std::make_shared<NullWrapperField<std::vector<char>>>(field, std::vector<char>(fallback.begin(), fallback.end()));

        default:
            throw SchemaError(fmt::format("Unsupported SchemaType: {}", static_cast<uint8_t>(field->get_type())));
        }
    }

    VirtualSchema::VirtualSchema(std::shared_ptr<ExtentSchema> extent_schema,
                                 const std::map<uint32_t, SchemaColumn> &columns,
                                 const std::vector<SchemaUpdate> &updates)
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
                    _field_map[update.name] = _make_null_wrapper(old_field, *(update.default_value));
                }
                break;
            case (SchemaUpdateType::TYPE_CHANGE):
                throw SchemaError("Can't handle column type changes yet");
            }
        }
    }

    FieldArrayPtr
    VirtualSchema::get_fields() const
    {
        std::vector<std::shared_ptr<Field>> _fields;

        for (auto &&i : _field_map) {
            _fields.push_back(i.second);
        }

        return std::make_shared<ReadFieldArray>(_fields);
    }

    FieldArrayPtr
    VirtualSchema::get_fields(const std::vector<std::string> &columns) const
    {
        std::vector<std::shared_ptr<Field>> _fields;

        for (auto &&name : columns) {
            _fields.push_back(this->get_field(name));
        }

        return std::make_shared<ReadFieldArray>(_fields);
    }

}
