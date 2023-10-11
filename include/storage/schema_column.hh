#pragma once

#include <map>
#include <string>
#include <vector>
#include <variant>

#include <storage/exception.hh>
#include <storage/schema_type.hh>

namespace springtail {
    // pre-declare classes
    class Field;

    /** Describes the details of a single column in the schema. */
    class SchemaColumnDetails {
    private:
        SchemaType _type; //< The data type of the column.
        bool _nullable; //< True if the column can be set to null.
        bool _default_null; //< True if the default value is null.

        /** The default value of the column if non-null. */
        std::variant<std::string,
                     uint64_t,
                     int64_t,
                     uint32_t,
                     int32_t,
                     uint16_t,
                     int16_t,
                     uint8_t,
                     int8_t,
                     double,
                     float,
                     bool> _default_value;

    public:
        SchemaColumnDetails(SchemaType type, bool nullable)
            : _type(type), _nullable(nullable), _default_null(true)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, const std::string &default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, uint64_t default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, int64_t default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, uint32_t default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, int32_t default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, uint16_t default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, int16_t default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, uint8_t default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, int8_t default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, double default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, float default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaColumnDetails(SchemaType type, bool nullable, bool default_value)
            : _type(type), _nullable(nullable), _default_null(false), _default_value(default_value)
        { }

        SchemaType type() const {
            return _type;
        }

        bool nullable() const {
            return _nullable;
        }

        bool default_null() const {
            return _default_null;
        }

        template<class T>
        T default_value() const {
            return std::get<T>(_default_value);
        }
    };

    /** Defines the interface for a single column in the schema.

        Key operations:
        - get_field() provides an accessor to the data in to the row for this column
     */
    class SchemaColumn {
    protected:
        SchemaType _type; ///< The type of the column.
        uint32_t _row_pos; ///< The position within the fixed row data.
        uint16_t _null_pos; ///< Defines the bit in the null columns for this column.
        bool _nullable; ///< Defines if the the field is nullable.
        bool _default_null; ///< Defines if the column has null as the default value.

    protected:
        /** Helper for updating the position information of columns. */
        void _position_helper(uint32_t &cur_byte, uint16_t &null_bit, uint8_t size);

    public:
        SchemaColumn(const SchemaColumnDetails &details);

        virtual ~SchemaColumn()
        {
            // intentionally empty
        }

        virtual void update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict) = 0;

        /** Function to call to get a Field accessor for this column. */
        virtual std::shared_ptr<Field> get_field(uint32_t bool_offset, uint32_t null_offset) = 0;
    };

    class SchemaColumnBoolean : public SchemaColumn {
    private:
        uint16_t _bool_pos; ///< The bit position within the bool data for this field
        bool _default_value; ///< The default value for this column (if it exists)

    public:
        SchemaColumnBoolean(const SchemaColumnDetails &details)
            : SchemaColumn(details),
              _default_value(details.default_value<bool>())
        { }

        virtual void update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
        virtual std::shared_ptr<Field> get_field(uint32_t bool_offset, uint32_t null_offset);
    };

    template <class T>
    class SchemaColumnNumber : public SchemaColumn {
    private:
        T _default_value; ///< The default value for this column (if it exists)

    public:
        SchemaColumnNumber(const SchemaColumnDetails &details)
            : SchemaColumn(details),
              _default_value(details.default_value<T>())
        { }

        virtual void update_position(uint32_t &cur_byte,
                                     uint16_t &bool_bit,
                                     uint16_t &null_bit,
                                     bool strict);

        virtual std::shared_ptr<Field> get_field(uint32_t bool_offset,
                                                 uint32_t null_offset);
    };


    class SchemaColumnText : public SchemaColumn {
    private:
        std::string _default_value;

    public:
        SchemaColumnText(const SchemaColumnDetails &details)
            : SchemaColumn(details),
              _default_value(details.default_value<std::string>())
        { }

        virtual void update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
        virtual std::shared_ptr<Field> get_field(uint32_t bool_offset, uint32_t null_offset);
    };

    class SchemaColumnBinary : public SchemaColumn {
    public:
        SchemaColumnBinary(const SchemaColumnDetails &details)
            : SchemaColumn(details)
        { }

        virtual void update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
        virtual std::shared_ptr<Field> get_field(uint32_t bool_offset, uint32_t null_offset);
    };

}
