#pragma once

#include <iostream>
#include <type_traits>

#include <storage/extent.hh>
#include <storage/schema_type.hh>

namespace st_storage {

    class Field {
    protected:
        uint32_t _offset; ///< The offset into the fixed data for the row.

    protected:
        // used by Nullable fields because of virtual inheritance
        Field()
        { }

    public:
        Field(uint32_t offset)
            : _offset(offset)
        { }

        virtual ~Field()
        { }

        virtual SchemaType get_type() = 0;

        // functions to read values

        virtual bool is_null(const Extent::Row &row) const {
            return false;
        }

        virtual bool get_bool(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting bool type unsupported for this field."));
        }

        virtual int8_t get_int8(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting int8 unsupported for this field."));
        }

        virtual uint8_t get_uint8(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting uint8 unsupported for this field."));
        }

        virtual int16_t get_int16(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting int16 unsupported for this field."));
        }

        virtual uint16_t get_uint16(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting uint16 unsupported for this field."));
        }

        virtual int32_t get_int32(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting int32 unsupported for this field."));
        }

        virtual uint32_t get_uint32(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting uint32 unsupported for this field."));
        }

        virtual int64_t get_int64(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting int64 unsupported for this field."));
        }

        virtual uint64_t get_uint64(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting uint64 unsupported for this field."));
        }

        virtual float get_float32(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting float unsupported for this field."));
        }

        virtual double get_float64(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting double unsupported for this field."));
        }

        virtual std::string get_text(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting string type unsupported for this field."));
        }

        virtual const std::vector<char> get_binary(const Extent::Row &row) const {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Getting binary type unsupported for this field."));
        }

        // functions to set values

        virtual void set_null(Extent::MutableRow &row, bool is_null) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting null unsupported for this field."));
        }

        virtual void set_bool(Extent::MutableRow &row, bool value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting bool unsupported for this field."));
        }

        virtual void set_int8(Extent::MutableRow &row, int8_t value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting int8 unsupported for this field."));
        }

        virtual void set_uint8(Extent::MutableRow &row, uint8_t value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting uint8 unsupported for this field."));
        }

        virtual void set_int16(Extent::MutableRow &row, int16_t value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting int16 unsupported for this field."));
        }

        virtual void set_uint16(Extent::MutableRow &row, uint16_t value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting uint16 unsupported for this field."));
        }

        virtual void set_int32(Extent::MutableRow &row, int32_t value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting int32 unsupported for this field."));
        }

        virtual void set_uint32(Extent::MutableRow &row, uint32_t value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting uint32 unsupported for this field."));
        }

        virtual void set_int64(Extent::MutableRow &row, int64_t value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting int64 unsupported for this field."));
        }

        virtual void set_uint64(Extent::MutableRow &row, uint64_t value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting uint64 unsupported for this field."));
        }

        virtual void set_text(Extent::MutableRow &row, const std::string &value) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting string unsupported for this field."));
        }

        virtual void set_binary(Extent::MutableRow &row, const std::vector<char> &val) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting binary type unsupported for this field."));
        }

        // set this field from the value of another field
        virtual void set_field(Extent::MutableRow &row,
                               const Extent::Row &input_row,
                               const Field &field) {
            BOOST_THROW_EXCEPTION(ErrorField()
                                  << ErrorMessage("Setting from a Field unsupported for this field."));
        }
    };

    class NullableField : virtual public Field {
    private:
        uint32_t _null_offset;
        char _null_mask;

    public:
        NullableField(uint32_t null_offset, uint8_t null_bit)
            : _null_offset(null_offset)
        {
            assert(null_bit < 8);
            _null_mask = static_cast<char>(1) << null_bit;
        }

        bool is_null(const Extent::Row &row) const {
            const char * const fixed = row.data + _null_offset;
            return ((*reinterpret_cast<const uint8_t * const>(fixed) & _null_mask) > 0);
        }

        void set_null(Extent::MutableRow &row, bool is_null) {
            char * const fixed = row.data + _null_offset;
            char mask = static_cast<char>(-1) & _null_mask;
            if (is_null) {
                *fixed |= mask;
            } else {
                *fixed &= ~mask;
            }
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       const Field &field)
        {
            this->set_null(row, field.is_null(input_row));
        }
    };

    class BoolField : virtual public Field {
    private:
        char _bit_mask; ///< The bitmask for the bit of this boolean

    public:
        BoolField(uint32_t offset, uint8_t bool_bit)
            : Field(offset)
        {
            assert(bool_bit < 8);
            _bit_mask = static_cast<char>(1) << bool_bit;
        }

        virtual SchemaType get_type() {
            return SchemaType::BOOLEAN;
        }

        bool get_bool(const Extent::Row &row) const {
            const char * const fixed = row.data + _offset;
            return ((*reinterpret_cast<const uint8_t * const>(fixed) & _bit_mask) > 0);
        }

        virtual void set_bool(Extent::MutableRow &row, bool val) {
            char *fixed = row.data + _offset;
            char mask = (static_cast<char>(-1) & _bit_mask);
            if (val) {
                *fixed |= mask;
            } else {
                *fixed &= ~mask;
            }
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       const Field &field)
        {
            this->set_bool(row, field.get_bool(input_row));
        }
    };

    class NullableBoolField : public BoolField, public NullableField {
    public:
        NullableBoolField(uint32_t offset, uint8_t bool_bit,
                          uint32_t null_offset, uint8_t null_bit)
            : BoolField(offset, bool_bit),
              NullableField(null_offset, null_bit)
        { }

        virtual SchemaType get_type() {
            return BoolField::get_type();
        }


        void set_bool(Extent::MutableRow &row, bool val) {
            BoolField::set_bool(row, val);
            NullableField::set_null(row, false);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       const Field &field)
        {
            BoolField::set_field(row, input_row, field);
            NullableField::set_field(row, input_row, field);
        }
    };

    /** Parent field type used for all numeric types (integers and floats) */
    template <class T>
    class NumberField : virtual public Field {
    protected:
        size_t _fixed_size;

    protected:
        T _get_number(const Extent::Row &row) const {
            T data;
            std::copy_n(row.data + _offset, _fixed_size, reinterpret_cast<char *>(&data));
            return data;
        }

        void _set_number(Extent::MutableRow &row, T val) {
            char *fixed = row.data + _offset;
            std::copy_n(reinterpret_cast<char *>(&val), _fixed_size, fixed);
        }

    public:
        NumberField(uint32_t offset)
            : Field(offset),
              _fixed_size(sizeof(T))
        {
            static_assert(std::is_arithmetic<T>::value,
                          "NumberField must be of an arithmetic type");
        }

        SchemaType get_type() {
            if constexpr(std::is_same<T, uint64_t>::value) {
                return SchemaType::UINT64;
            } else if constexpr(std::is_same_v<T, int64_t>) {
                return SchemaType::INT64;
            } else if constexpr(std::is_same_v<T, uint32_t>) {
                return SchemaType::UINT32;
            } else if constexpr(std::is_same_v<T, int32_t>) {
                return SchemaType::INT32;
            } else if constexpr(std::is_same_v<T, uint16_t>) {
                return SchemaType::UINT16;
            } else if constexpr(std::is_same_v<T, int16_t>) {
                return SchemaType::INT16;
            } else if constexpr(std::is_same_v<T, uint8_t>) {
                return SchemaType::UINT8;
            } else if constexpr(std::is_same_v<T, int8_t>) {
                return SchemaType::INT8;
            } else if constexpr(std::is_same_v<T, double>) {
                return SchemaType::FLOAT64;
            } else if constexpr(std::is_same_v<T, float>) {
                return SchemaType::FLOAT32;
            } else {
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Invalid NumberField type"));
            }
        }

        int8_t get_int8(const Extent::Row &row) const {
            return static_cast<int8_t>(_get_number(row));
        }

        uint8_t get_uint8(const Extent::Row &row) const {
            return static_cast<uint8_t>(_get_number(row));
        }

        int16_t get_int16(const Extent::Row &row) const {
            return static_cast<uint16_t>(_get_number(row));
        }

        uint16_t get_uint16(const Extent::Row &row) const {
            return static_cast<uint16_t>(_get_number(row));
        }

        int32_t get_int32(const Extent::Row &row) const {
            return static_cast<uint32_t>(_get_number(row));
        }

        uint32_t get_uint32(const Extent::Row &row) const {
            return static_cast<uint32_t>(_get_number(row));
        }

        int64_t get_int64(const Extent::Row &row) const {
            return static_cast<uint64_t>(_get_number(row));
        }

        uint64_t get_uint64(const Extent::Row &row) const {
            return static_cast<uint64_t>(_get_number(row));
        }

        float get_float32(const Extent::Row &row) const {
            return static_cast<float>(_get_number(row));
        }

        double get_float64(const Extent::Row &row) const {
            return static_cast<double>(_get_number(row));
        }

        void set_int8(Extent::MutableRow &row, int8_t val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_uint8(Extent::MutableRow &row, uint8_t val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_int16(Extent::MutableRow &row, int16_t val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_uint16(Extent::MutableRow &row, uint16_t val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_int32(Extent::MutableRow &row, int32_t val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_uint32(Extent::MutableRow &row, uint32_t val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_int64(Extent::MutableRow &row, int64_t val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_uint64(Extent::MutableRow &row, uint64_t val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_float32(Extent::MutableRow &row, float val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_float64(Extent::MutableRow &row, double val) {
            _set_number(row, static_cast<T>(val));
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       const Field &field)
        {
            if constexpr(std::is_same<T, uint64_t>::value) {
                this->set_uint64(row, field.get_uint64(input_row));
            } else if constexpr(std::is_same_v<T, int64_t>) {
                this->set_int64(row, field.get_int64(input_row));
            } else if constexpr(std::is_same_v<T, uint32_t>) {
                this->set_uint32(row, field.get_uint32(input_row));
            } else if constexpr(std::is_same_v<T, int32_t>) {
                this->set_int32(row, field.get_int32(input_row));
            } else if constexpr(std::is_same_v<T, uint16_t>) {
                this->set_uint16(row, field.get_uint16(input_row));
            } else if constexpr(std::is_same_v<T, int16_t>) {
                this->set_int16(row, field.get_int16(input_row));
            } else if constexpr(std::is_same_v<T, uint8_t>) {
                this->set_uint8(row, field.get_uint8(input_row));
            } else if constexpr(std::is_same_v<T, int8_t>) {
                this->set_int8(row, field.get_int8(input_row));
            } else if constexpr(std::is_same_v<T, double>) {
                this->set_float64(row, field.get_float64(input_row));
            } else if constexpr(std::is_same_v<T, float>) {
                this->set_float32(row, field.get_float32(input_row));
            } else {
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Invalid NumberField type"));
            }
        }
    };

    template <class T>
    class NullableNumberField : public NumberField<T>, public NullableField {
    public:
        NullableNumberField(uint32_t offset,
                            uint32_t null_offset, uint8_t null_bit)
            : NumberField<T>(offset),
              NullableField(null_offset, null_bit)
        { }

        virtual SchemaType get_type() {
            return NumberField<T>::get_type();
        }

        void set_int8(Extent::MutableRow &row, int8_t val) {
            NumberField<T>::set_int8(row, val);
            NullableField::set_null(row, false);
        }

        void set_uint8(Extent::MutableRow &row, uint8_t val) {
            NumberField<T>::set_uint8(row, val);
            NullableField::set_null(row, false);
        }

        void set_int16(Extent::MutableRow &row, int16_t val) {
            NumberField<T>::set_int16(row, val);
            NullableField::set_null(row, false);
        }

        void set_uint16(Extent::MutableRow &row, uint16_t val) {
            NumberField<T>::set_uint16(row, val);
            NullableField::set_null(row, false);
        }

        void set_int32(Extent::MutableRow &row, int32_t val) {
            NumberField<T>::set_int32(row, val);
            NullableField::set_null(row, false);
        }

        void set_uint32(Extent::MutableRow &row, uint32_t val) {
            NumberField<T>::set_uint32(row, val);
            NullableField::set_null(row, false);
        }

        void set_int64(Extent::MutableRow &row, int64_t val) {
            NumberField<T>::set_int64(row, val);
            NullableField::set_null(row, false);
        }

        void set_uint64(Extent::MutableRow &row, uint64_t val) {
            NumberField<T>::set_uint64(row, val);
            NullableField::set_null(row, false);
        }

        void set_float32(Extent::MutableRow &row, float val) {
            NumberField<T>::set_float32(row, val);
            NullableField::set_null(row, false);
        }

        void set_float64(Extent::MutableRow &row, double val) {
            NumberField<T>::set_float64(row, val);
            NullableField::set_null(row, false);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       const Field &field)
        {
            NumberField<T>::set_field(row, input_row, field);
            NullableField::set_field(row, input_row, field);
        }
    };

    class TextField : virtual public Field {
    public:
        TextField(uint32_t offset)
            : Field(offset)
        { }

        SchemaType get_type() {
            return SchemaType::TEXT;
        }

        std::string get_text(const Extent::Row &row) const {
            // get the offset into the variable data
            uint32_t offset;
            std::copy_n(row.data + _offset, sizeof(uint32_t), reinterpret_cast<char *>(&offset));

            // retrieve the string from the variable data
            return row.extent->get_text(offset);
        }

        void set_text(Extent::MutableRow &row, const std::string &value) {
            // store the string into the variable data
            uint32_t offset = row.set_text(value);

            // store the offset into the fixed data
            char *fixed = row.data + _offset;
            std::copy_n(reinterpret_cast<char *>(&offset), sizeof(uint32_t), fixed);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       const Field &field)
        {
            // XXX if the field is a TEXT or BINARY then we want to do a direct copy

            // otherwise we can just call get_text()
            this->set_text(row, field.get_text(input_row));
        }
    };

    class NullableTextField : public TextField, public NullableField {
    public:
        NullableTextField(uint32_t offset, uint32_t null_offset, uint8_t null_bit)
            : TextField(offset),
              NullableField(null_offset, null_bit)
        { }

        virtual SchemaType get_type() {
            return TextField::get_type();
        }

        void set_text(Extent::MutableRow &row, const std::string &value) {
            TextField::set_text(row, value);
            NullableField::set_null(row, false);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       const Field &field)
        {
            TextField::set_field(row, input_row, field);
            NullableField::set_field(row, input_row, field);
        }
    };

    class BinaryField : virtual public Field {
    public:
        BinaryField(uint32_t offset)
            : Field(offset)
        { }

        SchemaType get_type() {
            return SchemaType::BINARY;
        }

        const std::vector<char> get_binary(const Extent::Row &row) const {
            uint32_t offset;

            // get the offset into the variable data
            std::copy_n(row.data + _offset, sizeof(uint32_t), reinterpret_cast<char *>(&offset));

            // retrieve the binary data from the variable data
            return row.extent->get_binary(offset);
        }

        void set_binary(Extent::MutableRow &row, const std::vector<char> &val) {
            // store the data into the variable-sized data
            uint32_t offset = row.set_binary(val);

            // copy the offset into the fixed data
            char *fixed = row.data + _offset;
            std::copy_n(reinterpret_cast<char *>(&offset), sizeof(uint32_t), fixed);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       const Field &field)
        {
            // XXX if the field is a TEXT or BINARY then we want to do a direct copy

            // otherwise we can just call get_binary()
            this->set_binary(row, field.get_binary(input_row));
        }
    };

    class NullableBinaryField : public BinaryField, public NullableField {
    public:
        NullableBinaryField(uint32_t offset, uint32_t null_offset, uint8_t null_bit)
            : BinaryField(offset),
              NullableField(null_offset, null_bit)
        { }

        virtual SchemaType get_type() {
            return BinaryField::get_type();
        }

        void set_binary(Extent::MutableRow &row, const std::vector<char> &val) {
            BinaryField::set_binary(row, val);
            NullableField::set_null(row, false);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       const Field &field)
        {
            BinaryField::set_field(row, input_row, field);
            NullableField::set_field(row, input_row, field);
        }
    };

    class AnyField : virtual public Field {
    private:
        template <class T>
        T _get_integer(const Extent::Row &row) const {
            switch (_get_data_type(row)) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::UINT16):
            case (SchemaType::INT16):
            case (SchemaType::UINT8):
            case (SchemaType::INT8):
            case (SchemaType::BOOLEAN):
            case (SchemaType::FLOAT64):
            case (SchemaType::FLOAT32):
                return static_cast<T>(_get_data(row));

            case (SchemaType::DECIMAL128):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Decimal128 unsupported"));
                
            case (SchemaType::TEXT):
                // XXX try to convert the string to a numeric?
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Text type cannot be auto-cast to an integer"));

            case (SchemaType::BINARY):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Binary type cannot be cast to an integer"));

            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                // XXX use the length of the container?
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Container types cannot be cast to an integer"));
            }            
        }

        template <class T>
        T _get_float(const Extent::Row &row) const {
            uint64_t data = _get_data(row);

            switch (_get_data_type(row)) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::UINT16):
            case (SchemaType::INT16):
            case (SchemaType::UINT8):
            case (SchemaType::INT8):
            case (SchemaType::BOOLEAN):
                return static_cast<T>(data);

            case (SchemaType::FLOAT64):
                return reinterpret_cast<double &>(data);

            case (SchemaType::FLOAT32): {
                uint32_t data32 = static_cast<uint32_t>(data);
                return reinterpret_cast<float &>(data32);
            }

            case (SchemaType::DECIMAL128):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Decimal128 unsupported"));
                
            case (SchemaType::TEXT):
                // XXX try to convert the string to a numeric?
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Text type cannot be auto-cast to a float"));

            case (SchemaType::BINARY):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Binary type cannot be cast to a float"));

            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                // XXX use the length of the container?
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Container types cannot be cast to float"));
            }            
        }

        SchemaType _get_data_type(const Extent::Row &row) const {
            // retrieve the type of the field for this row
            return *reinterpret_cast<const SchemaType *>(row.data + _offset);
        }

        void _set_data_type(Extent::MutableRow &row, SchemaType type) {
            // set the type of the field for this row
            *reinterpret_cast<SchemaType *>(row.data + _offset) = type;
        }

        uint64_t _get_data(const Extent::Row &row) const {
            // retrieve the 64-bit data of the field for this row
            uint64_t any_data;
            std::copy_n(row.data + _offset + 1, sizeof(uint64_t),
                        reinterpret_cast<char *>(&any_data));
            return any_data;
        }

        void _set_data(Extent::MutableRow &row, uint64_t value) {
            // store the provided value into the field for this row
            std::copy_n(reinterpret_cast<char *>(&value),
                        sizeof(uint64_t), row.data + _offset + 1);
        }

    public:
        AnyField(uint32_t offset)
            : Field(offset)
        { }

        SchemaType get_type() {
            return SchemaType::ANY;
        }

        bool get_bool(const Extent::Row &row) const {
            // in the case of ANY type we try to force a cast since the type could be different from
            // row to row
            switch (_get_data_type(row)) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::UINT16):
            case (SchemaType::INT16):
            case (SchemaType::UINT8):
            case (SchemaType::INT8):
            case (SchemaType::BOOLEAN):
            case (SchemaType::FLOAT64):
            case (SchemaType::FLOAT32):
                return static_cast<bool>(_get_data(row));

            case (SchemaType::DECIMAL128):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Decimal128 unsupported"));
                
            case (SchemaType::BINARY):
            case (SchemaType::TEXT):
                return true;

            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                // XXX check the length of the container and return true if non-zero
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Container types cannot be cast to boolean"));
            }
        }

        virtual int8_t get_int8(const Extent::Row &row) const {
            return _get_integer<int8_t>(row);
        }

        virtual uint8_t get_uint8(const Extent::Row &row) const {
            return _get_integer<uint8_t>(row);
        }

        virtual int16_t get_int16(const Extent::Row &row) const {
            return _get_integer<int16_t>(row);
        }

        virtual uint16_t get_uint16(const Extent::Row &row) const {
            return _get_integer<uint16_t>(row);
        }

        virtual int32_t get_int32(const Extent::Row &row) const {
            return _get_integer<int32_t>(row);
        }

        virtual uint32_t get_uint32(const Extent::Row &row) const {
            return _get_integer<uint32_t>(row);
        }

        virtual int64_t get_int64(const Extent::Row &row) const {
            return _get_integer<int64_t>(row);
        }

        virtual uint64_t get_uint64(const Extent::Row &row) const {
            return _get_integer<uint64_t>(row);
        }

        virtual float get_float32(const Extent::Row &row) const {
            return _get_float<float>(row);
        }

        virtual double get_float64(const Extent::Row &row) const {
            return _get_float<double>(row);
        }

        virtual std::string get_text(const Extent::Row &row) const {
            switch (_get_data_type(row)) {
            case (SchemaType::UINT64):
                return std::to_string(_get_integer<uint64_t>(row));
            case (SchemaType::INT64):
                return std::to_string(_get_integer<int64_t>(row));
            case (SchemaType::UINT32):
                return std::to_string(_get_integer<uint32_t>(row));
            case (SchemaType::INT32):
                return std::to_string(_get_integer<int32_t>(row));
            case (SchemaType::UINT16):
                return std::to_string(_get_integer<uint16_t>(row));
            case (SchemaType::INT16):
                return std::to_string(_get_integer<int16_t>(row));
            case (SchemaType::UINT8):
                return std::to_string(_get_integer<uint8_t>(row));
            case (SchemaType::INT8):
                return std::to_string(_get_integer<int8_t>(row));
            case (SchemaType::FLOAT64):
                return std::to_string(_get_float<double>(row));
            case (SchemaType::FLOAT32):
                return std::to_string(_get_float<float>(row));

            case (SchemaType::BOOLEAN):
                return _get_data(row) ? "true" : "false";

            case (SchemaType::TEXT): {
                uint64_t offset = _get_data(row);

                // retrieve the string from the variable data
                return row.extent->get_text(offset);
            }

            case (SchemaType::DECIMAL128):
            case (SchemaType::BINARY): // XXX could consider auto-casting binary data to string
            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                return ""; // unsupported conversions
            }
        }

        virtual const std::vector<char> get_binary(const Extent::Row &row) const {
            std::vector<char> binary_data;

            // copy the bytes of the value into the binary vector
            switch (_get_data_type(row)) {
            case (SchemaType::UINT64): {
                uint64_t data = _get_integer<uint64_t>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(uint64_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::INT64): {
                int64_t data = _get_integer<int64_t>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(int64_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::UINT32): {
                uint32_t data = _get_integer<uint32_t>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(uint32_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::INT32): {
                int32_t data = _get_integer<int32_t>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(int32_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::UINT16): {
                uint16_t data = _get_integer<uint16_t>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(uint16_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::INT16): {
                int16_t data = _get_integer<int16_t>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(int16_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::UINT8): {
                uint8_t data = _get_integer<uint8_t>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(uint8_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::INT8): {
                int8_t data = _get_integer<int8_t>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(int8_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::FLOAT64): {
                double data = _get_float<double>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(double), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::FLOAT32): {
                float data = _get_float<float>(row);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(float), std::back_inserter(binary_data));
            }
                break;

            case (SchemaType::BOOLEAN):
                binary_data.push_back(_get_data(row) ? 0 : 1);
                break;

            case (SchemaType::BINARY):
            case (SchemaType::TEXT): {
                // binary data and text is stored in the same format
                uint64_t offset = _get_data(row);

                // retrieve the binary data from the variable data
                return row.extent->get_binary(offset);
            }

            case (SchemaType::DECIMAL128):
            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                return std::vector<char>(); // unsupported conversions
            }

            return binary_data;
        }

        virtual void set_bool(Extent::MutableRow &row, bool value) {
            _set_data_type(row, SchemaType::BOOLEAN);
            _set_data(row, value);
        }

        virtual void set_int8(Extent::MutableRow &row, int8_t value) {
            _set_data_type(row, SchemaType::INT8);
            _set_data(row, value);
        }

        virtual void set_uint8(Extent::MutableRow &row, uint8_t value) {
            _set_data_type(row, SchemaType::UINT8);
            _set_data(row, value);
        }

        virtual void set_int16(Extent::MutableRow &row, int16_t value) {
            _set_data_type(row, SchemaType::INT16);
            _set_data(row, value);
        }

        virtual void set_uint16(Extent::MutableRow &row, uint16_t value) {
            _set_data_type(row, SchemaType::UINT16);
            _set_data(row, value);
        }

        virtual void set_int32(Extent::MutableRow &row, int32_t value) {
            _set_data_type(row, SchemaType::INT32);
            _set_data(row, value);
        }

        virtual void set_uint32(Extent::MutableRow &row, uint32_t value) {
            _set_data_type(row, SchemaType::UINT32);
            _set_data(row, value);
        }

        virtual void set_int64(Extent::MutableRow &row, int64_t value) {
            _set_data_type(row, SchemaType::INT64);
            _set_data(row, value);
        }

        virtual void set_uint64(Extent::MutableRow &row, uint64_t value) {
            _set_data_type(row, SchemaType::UINT64);
            _set_data(row, value);
        }

        virtual void set_float32(Extent::MutableRow &row, float value) {
            uint64_t data = *reinterpret_cast<uint32_t *>(&value);
            _set_data_type(row, SchemaType::FLOAT32);
            _set_data(row, data);
        }

        virtual void set_float64(Extent::MutableRow &row, double value) {
            uint64_t data = *reinterpret_cast<uint64_t *>(&value);
            _set_data_type(row, SchemaType::FLOAT64);
            _set_data(row, data);
        }

        virtual void set_text(Extent::MutableRow &row, const std::string &value) {
            // store the string into the variable data
            uint32_t offset = row.set_text(value);

            // store the offset into the field
            _set_data_type(row, SchemaType::TEXT);
            _set_data(row, offset);
        }

        virtual void set_binary(Extent::MutableRow &row, const std::vector<char> &value) {
            // store the string into the variable data
            uint32_t offset = row.set_binary(value);

            // store the offset into the field
            _set_data_type(row, SchemaType::BINARY);
            _set_data(row, offset);
        }
    };

    class NullableAnyField : public AnyField, public NullableField {
    public:
        NullableAnyField(uint32_t offset, uint32_t null_offset, uint8_t null_bit)
            : AnyField(offset),
              NullableField(null_offset, null_bit)
        { }

        virtual SchemaType get_type() {
            return AnyField::get_type();
        }

        void set_bool(Extent::MutableRow &row, bool value) {
            AnyField::set_bool(row, value);
            NullableField::set_null(row, false);
        }

        void set_int8(Extent::MutableRow &row, int8_t value) {
            AnyField::set_int8(row, value);
            NullableField::set_null(row, false);
        }

        void set_uint8(Extent::MutableRow &row, uint8_t value) {
            AnyField::set_uint8(row, value);
            NullableField::set_null(row, false);
        }

        void set_int16(Extent::MutableRow &row, int16_t value) {
            AnyField::set_int16(row, value);
            NullableField::set_null(row, false);
        }

        void set_uint16(Extent::MutableRow &row, uint16_t value) {
            AnyField::set_uint16(row, value);
            NullableField::set_null(row, false);
        }

        void set_int32(Extent::MutableRow &row, int32_t value) {
            AnyField::set_int32(row, value);
            NullableField::set_null(row, false);
        }

        void set_uint32(Extent::MutableRow &row, uint32_t value) {
            AnyField::set_uint32(row, value);
            NullableField::set_null(row, false);
        }

        void set_int64(Extent::MutableRow &row, int64_t value) {
            AnyField::set_int64(row, value);
            NullableField::set_null(row, false);
        }

        void set_uint64(Extent::MutableRow &row, uint64_t value) {
            AnyField::set_uint64(row, value);
            NullableField::set_null(row, false);
        }

        void set_float32(Extent::MutableRow &row, float value) {
            AnyField::set_float32(row, value);
            NullableField::set_null(row, false);
        }

        void set_float64(Extent::MutableRow &row, double value) {
            AnyField::set_float64(row, value);
            NullableField::set_null(row, false);
        }

        void set_binary(Extent::MutableRow &row, const std::vector<char> &val) {
            AnyField::set_binary(row, val);
            NullableField::set_null(row, false);
        }
    };

    class UndefinedField : virtual public Field {
    private:
        std::vector<std::string> _key; ///< The key for this optional column

    protected:
        template <class T>
        T _get_integer(const Extent::Row &row) const {
            uint32_t offset = row.find_untyped(_key);
            if (!offset) {
                // is null, so 0 is default
                return 0;
            }

            return _get_integer<T>(row, offset);
        }

        template <class T>
        T _get_integer(const Extent::Row &row, uint32_t offset) const {
            switch (row.extent->get_untyped_type(offset)) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::UINT16):
            case (SchemaType::INT16):
            case (SchemaType::UINT8):
            case (SchemaType::INT8):
            case (SchemaType::BOOLEAN):
            case (SchemaType::FLOAT64):
            case (SchemaType::FLOAT32):
                return static_cast<T>(row.extent->get_untyped_value(offset));

            case (SchemaType::DECIMAL128):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Decimal128 unsupported"));
                
            case (SchemaType::TEXT):
                // XXX try to convert the string to a numeric?
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Text type cannot be auto-cast to an integer"));

            case (SchemaType::BINARY):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Binary type cannot be cast to an integer"));

            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                // XXX use the length of the container?
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Container types cannot be cast to an integer"));
            }            
        }

        template <class T>
        T _get_float(const Extent::Row &row) const {
            uint32_t offset = row.find_untyped(_key);
            if (!offset) {
                // is null, so 0 is default
                return 0;
            }

            return _get_float<T>(row, offset);
        }

        template <class T>
        T _get_float(const Extent::Row &row, uint32_t offset) const {
            uint64_t data = row.extent->get_untyped_value(offset);

            switch (row.extent->get_untyped_type(offset)) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::UINT16):
            case (SchemaType::INT16):
            case (SchemaType::UINT8):
            case (SchemaType::INT8):
            case (SchemaType::BOOLEAN):
                return static_cast<T>(data);

            case (SchemaType::FLOAT64):
                return reinterpret_cast<double &>(data);

            case (SchemaType::FLOAT32): {
                uint32_t data32 = static_cast<uint32_t>(data);
                return reinterpret_cast<float &>(data32);
            }

            case (SchemaType::DECIMAL128):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Decimal128 unsupported"));
                
            case (SchemaType::TEXT):
                // XXX try to convert the string to a numeric?
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Text type cannot be auto-cast to a float"));

            case (SchemaType::BINARY):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Binary type cannot be cast to a float"));

            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                // XXX use the length of the container?
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Container types cannot be cast to float"));
            }            
        }

    public:
        /** Takes in the name of the field and the offset to the position of the untyped data
            pointer in the row. */
        UndefinedField(const std::vector<std::string> &key, uint32_t offset)
            : Field(offset), _key(key)
        { }

        SchemaType get_type() {
            return SchemaType::ANY;
        }

        bool is_null(const Extent::Row &row) const {
            // we don't store null undefined column data... abscence of a field is equivalent to null
            // XXX should we change this to support storing explicit nulls?

            // XXX should we somehow cache this to avoid calling it again in the get*() call?  could be expensive
            uint32_t offset = row.find_untyped(_key);
            if (!offset) {
                return true;
            }

            return false;
        }

        bool get_bool(const Extent::Row &row) const {
            uint32_t offset = row.find_untyped(_key);
            if (!offset) {
                // is null, so false is default
                return false;
            }

            // in the case of ANY type we try to force a cast since the type could be different from
            // row to row
            switch (row.extent->get_untyped_type(offset)) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::UINT16):
            case (SchemaType::INT16):
            case (SchemaType::UINT8):
            case (SchemaType::INT8):
            case (SchemaType::BOOLEAN):
            case (SchemaType::FLOAT64):
            case (SchemaType::FLOAT32):
                return static_cast<bool>(row.extent->get_untyped_value(offset));

            case (SchemaType::DECIMAL128):
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Decimal128 unsupported"));
                
            case (SchemaType::BINARY):
            case (SchemaType::TEXT):
                return true;

            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                // XXX check the length of the container and return true if non-zero
                BOOST_THROW_EXCEPTION(ErrorField()
                                      << ErrorMessage("Container types cannot be cast to boolean"));
            }
        }

        virtual int8_t get_int8(const Extent::Row &row) const {
            return _get_integer<int8_t>(row);
        }

        virtual uint8_t get_uint8(const Extent::Row &row) const {
            return _get_integer<uint8_t>(row);
        }

        virtual int16_t get_int16(const Extent::Row &row) const {
            return _get_integer<int16_t>(row);
        }

        virtual uint16_t get_uint16(const Extent::Row &row) const {
            return _get_integer<uint16_t>(row);
        }

        virtual int32_t get_int32(const Extent::Row &row) const {
            return _get_integer<int32_t>(row);
        }

        virtual uint32_t get_uint32(const Extent::Row &row) const {
            return _get_integer<uint32_t>(row);
        }

        virtual int64_t get_int64(const Extent::Row &row) const {
            return _get_integer<int64_t>(row);
        }

        virtual uint64_t get_uint64(const Extent::Row &row) const {
            return _get_integer<uint64_t>(row);
        }

        virtual float get_float32(const Extent::Row &row) const {
            return _get_float<float>(row);
        }

        virtual double get_float64(const Extent::Row &row) const {
            return _get_float<double>(row);
        }

        virtual std::string get_text(const Extent::Row &row) const {
            uint32_t offset = row.find_untyped(_key);
            if (!offset) {
                // is null, so "" is default
                return "";
            }

            switch (row.extent->get_untyped_type(offset)) {
            case (SchemaType::UINT64):
                return std::to_string(_get_integer<uint64_t>(row, offset));
            case (SchemaType::INT64):
                return std::to_string(_get_integer<int64_t>(row, offset));
            case (SchemaType::UINT32):
                return std::to_string(_get_integer<uint32_t>(row, offset));
            case (SchemaType::INT32):
                return std::to_string(_get_integer<int32_t>(row, offset));
            case (SchemaType::UINT16):
                return std::to_string(_get_integer<uint16_t>(row, offset));
            case (SchemaType::INT16):
                return std::to_string(_get_integer<int16_t>(row, offset));
            case (SchemaType::UINT8):
                return std::to_string(_get_integer<uint8_t>(row, offset));
            case (SchemaType::INT8):
                return std::to_string(_get_integer<int8_t>(row, offset));
            case (SchemaType::FLOAT64):
                return std::to_string(_get_float<double>(row, offset));
            case (SchemaType::FLOAT32):
                return std::to_string(_get_float<float>(row, offset));

            case (SchemaType::BOOLEAN):
                return row.extent->get_untyped_value(offset) ? "true" : "false";

            case (SchemaType::TEXT): {
                uint64_t variable_offset = row.extent->get_untyped_value(offset);

                // retrieve the string from the variable data
                return row.extent->get_text(variable_offset);
            }

            case (SchemaType::DECIMAL128):
            case (SchemaType::BINARY): // XXX could consider auto-casting binary data to string
            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                return ""; // unsupported conversions
            }
        }

        virtual const std::vector<char> get_binary(const Extent::Row &row) const {
            std::vector<char> binary_data;

            uint32_t offset = row.find_untyped(_key);
            if (!offset) {
                // is null, so empty is default
                return binary_data;
            }

            // copy the bytes of the value into the binary vector
            switch (row.extent->get_untyped_type(offset)) {
            case (SchemaType::UINT64): {
                uint64_t data = _get_integer<uint64_t>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(uint64_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::INT64): {
                int64_t data = _get_integer<int64_t>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(int64_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::UINT32): {
                uint32_t data = _get_integer<uint32_t>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(uint32_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::INT32): {
                int32_t data = _get_integer<int32_t>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(int32_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::UINT16): {
                uint16_t data = _get_integer<uint16_t>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(uint16_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::INT16): {
                int16_t data = _get_integer<int16_t>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(int16_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::UINT8): {
                uint8_t data = _get_integer<uint8_t>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(uint8_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::INT8): {
                int8_t data = _get_integer<int8_t>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(int8_t), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::FLOAT64): {
                double data = _get_float<double>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(double), std::back_inserter(binary_data));
            }
                break;
            case (SchemaType::FLOAT32): {
                float data = _get_float<float>(row, offset);
                std::copy_n(reinterpret_cast<const char *>(&data),
                            sizeof(float), std::back_inserter(binary_data));
            }
                break;

            case (SchemaType::BOOLEAN):
                binary_data.push_back(row.extent->get_untyped_value(offset) ? 0 : 1);
                break;

            case (SchemaType::BINARY):
            case (SchemaType::TEXT): {
                // binary data and text is stored in the same format
                uint64_t variable_offset = row.extent->get_untyped_value(offset);

                // retrieve the binary data from the variable data
                row.extent->get_binary(variable_offset);
            }

            case (SchemaType::DECIMAL128):
            case (SchemaType::ARRAY):
            case (SchemaType::DICTIONARY):
            default:
                return std::vector<char>(); // unsupported conversions
            }

            return binary_data;
        }

        virtual void set_bool(Extent::MutableRow &row, bool value) {
            row.set_untyped(_key, SchemaType::BOOLEAN, value);
        }

        virtual void set_int8(Extent::MutableRow &row, int8_t value) {
            row.set_untyped(_key, SchemaType::INT8, value);
        }

        virtual void set_uint8(Extent::MutableRow &row, uint8_t value) {
            row.set_untyped(_key, SchemaType::UINT8, value);
        }

        virtual void set_int16(Extent::MutableRow &row, int16_t value) {
            row.set_untyped(_key, SchemaType::INT16, value);
        }

        virtual void set_uint16(Extent::MutableRow &row, uint16_t value) {
            row.set_untyped(_key, SchemaType::UINT16, value);
        }

        virtual void set_int32(Extent::MutableRow &row, int32_t value) {
            row.set_untyped(_key, SchemaType::INT32, value);
        }

        virtual void set_uint32(Extent::MutableRow &row, uint32_t value) {
            row.set_untyped(_key, SchemaType::UINT32, value);
        }

        virtual void set_int64(Extent::MutableRow &row, int64_t value) {
            row.set_untyped(_key, SchemaType::INT64, value);
        }

        virtual void set_uint64(Extent::MutableRow &row, uint64_t value) {
            row.set_untyped(_key, SchemaType::UINT64, value);
        }

        virtual void set_float32(Extent::MutableRow &row, float value) {
            uint64_t data = *reinterpret_cast<uint32_t *>(&value);
            row.set_untyped(_key, SchemaType::FLOAT32, data);
        }

        virtual void set_float64(Extent::MutableRow &row, double value) {
            uint64_t data = *reinterpret_cast<uint64_t *>(&value);
            row.set_untyped(_key, SchemaType::FLOAT64, data);
        }

        virtual void set_text(Extent::MutableRow &row, const std::string &value) {
            // store the string into the variable data
            uint32_t variable_offset = row.set_text(value);
            row.set_untyped(_key, SchemaType::TEXT, variable_offset);
        }

        virtual void set_binary(Extent::MutableRow &row, const std::vector<char> &value) {
            // store the binary data into the variable data
            uint32_t variable_offset = row.set_binary(value);
            row.set_untyped(_key, SchemaType::BINARY, variable_offset);
        }
    };

}
