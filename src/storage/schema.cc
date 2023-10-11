#include <fmt/core.h>

#include <storage/exception.hh>
#include <storage/schema.hh>
#include <storage/field.hh>

namespace springtail {

    Schema::Schema(uint64_t id)
        : _bool_bit_pos(0),
          _null_bit_pos(0),
          _id(id)
    {
        // intentionally empty
    }

    Schema::Schema(const Schema &schema)
        : _bool_bit_pos(schema._bool_bit_pos),
          _null_bit_pos(schema._null_bit_pos),
          _columns(schema._columns),
          _id(schema._id)
    {
        // intentionally empty
    }

    std::shared_ptr<SchemaColumn>
    Schema::_create_column(const SchemaColumnDetails &details)
    {
        switch (details.type()) {
        case SchemaType::BOOLEAN:
            return std::make_shared<SchemaColumnBoolean>(details);

        case SchemaType::TEXT:
            return std::make_shared<SchemaColumnText>(details);

        case SchemaType::UINT64:
            return std::make_shared<SchemaColumnNumber<uint64_t> >(details);

        case SchemaType::INT64:
            return std::make_shared<SchemaColumnNumber<int64_t> >(details);

        case SchemaType::UINT32:
            return std::make_shared<SchemaColumnNumber<uint32_t> >(details);

        case SchemaType::INT32:
            return std::make_shared<SchemaColumnNumber<int32_t> >(details);

        case SchemaType::UINT16:
            return std::make_shared<SchemaColumnNumber<uint16_t> >(details);

        case SchemaType::INT16:
            return std::make_shared<SchemaColumnNumber<int16_t> >(details);

        case SchemaType::UINT8:
            return std::make_shared<SchemaColumnNumber<uint8_t> >(details);

        case SchemaType::INT8:
            return std::make_shared<SchemaColumnNumber<int8_t> >(details);

        case SchemaType::FLOAT64:
            return std::make_shared<SchemaColumnNumber<double> >(details);

        case SchemaType::FLOAT32:
            return std::make_shared<SchemaColumnNumber<float> >(details);

        case SchemaType::BINARY:
            return std::make_shared<SchemaColumnBinary>(details);

        case SchemaType::DECIMAL128:
        default:
            std::cerr << fmt::format("Unsupported SchemaColumn type {:d}", static_cast<int>(details.type())) << std::endl;
            throw TypeError("Unsupported schema column type");
        }
    }

    void
    Schema::add_column(const std::string &name,
                       const SchemaColumnDetails &details)
    {
        // verify that the column name isn't already in use
        auto &&i = _name_map.find(name);
        if (i == _name_map.end()) {
            throw SchemaError(fmt::format("Column already exists {}", name));
        }

        // construct the column based on the details
        std::shared_ptr<SchemaColumn> column = _create_column(details);

        // add the column
        _name_map[name] = _columns.size();
        _columns.push_back(column);

        // update the column positions
        _update_positions();
    }

    void
    Schema::remove_column(const std::string &name)
    {
        auto &&i = _name_map.find(name);
        if (i == _name_map.end()) {
            throw SchemaError(fmt::format("Column does not exist {}", name));
        }

        std::shared_ptr<SchemaColumn> column = _columns[i->second];

        // remove the references to the column
        _columns.erase(_columns.begin() + i->second);
        _name_map.erase(i);

        // update the column positions
        _update_positions();
    }

    void
    Schema::alter_column(const std::string &from,
                         const std::string &to,
                         const SchemaColumnDetails &details)
    {
        // get the existing column details
        auto &&i = _name_map.find(from);
        if (i == _name_map.end()) {
            throw SchemaError(fmt::format("Column does not exist {}", from));
        }
        std::shared_ptr<SchemaColumn> column = _columns[i->second];

        // check for rename
        if (from != to) {
            // check that the new name doesn't already exist
            i = _name_map.find(to);
            if (i != _name_map.end()) {
                throw SchemaError(fmt::format("Column already exists {}", to));
            }

            // rename the column
            uint32_t index = i->second;
            _name_map.erase(i);
            _name_map[to] = index;
        }

        // XXX alter the column details
        // column->alter(details);

        // update the column positions
        _update_positions();
    }

    std::shared_ptr<Field>
    Schema::get_field(const std::string &name)
    {
        // get the column details
        auto &&i = _name_map.find(name);
        if (i == _name_map.end()) {
            throw SchemaError(fmt::format("Column does not exist {}", name));
        }
        std::shared_ptr<SchemaColumn> column = _columns[i->second];
        
        // return the field accessor for this column
        return column->get_field(_bool_bit_pos, _null_bit_pos);
    }

}
