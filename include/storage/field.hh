#pragma once

#include <iostream>
#include <type_traits>
#include <cassert>

#include <storage/extent.hh>
#include <storage/schema_type.hh>

namespace springtail {

    /**
     * The Field class is to create an accessor object for a given column of an extent.  It stores
     * the position of the column within the extent's fixed data and then interprets it based on the
     * type of the column.
     *
     * The data of a column in an extent is extracted by passing an Extent::Row to the Field's
     * approrpiate get_* function.  Calling the wrong function for a column of a given type will
     * result in an exception being thrown, so the caller must ensure that the type is compatible
     * with the get_* function being called.
     */
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
            std::cerr << "Getting bool type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual int8_t get_int8(const Extent::Row &row) const {
            std::cerr << "Getting int8 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual uint8_t get_uint8(const Extent::Row &row) const {
            std::cerr << "Getting uint8 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual int16_t get_int16(const Extent::Row &row) const {
            std::cerr << "Getting int16 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual uint16_t get_uint16(const Extent::Row &row) const {
            std::cerr << "Getting uint16 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual int32_t get_int32(const Extent::Row &row) const {
            std::cerr << "Getting int32 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual uint32_t get_uint32(const Extent::Row &row) const {
            std::cerr << "Getting uint32 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual int64_t get_int64(const Extent::Row &row) const {
            std::cerr << "Getting int64 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual uint64_t get_uint64(const Extent::Row &row) const {
            std::cerr << "Getting uint64 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual float get_float32(const Extent::Row &row) const {
            std::cerr << "Getting float32 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual double get_float64(const Extent::Row &row) const {
            std::cerr << "Getting float64 type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual std::string get_text(const Extent::Row &row) const {
            std::cerr << "Getting text type unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual const std::vector<char> get_binary(const Extent::Row &row) const {
            std::cerr << "Getting binary type unsupported for this field." << std::endl;
            throw TypeError();
        }

        // functions to set values

        virtual void set_null(Extent::MutableRow &row, bool is_null) {
            std::cerr << "Setting null unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_bool(Extent::MutableRow &row, bool value) {
            std::cerr << "Setting bool unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_int8(Extent::MutableRow &row, int8_t value) {
            std::cerr << "Setting int8 unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_uint8(Extent::MutableRow &row, uint8_t value) {
            std::cerr << "Setting uint8 unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_int16(Extent::MutableRow &row, int16_t value) {
            std::cerr << "Setting int16 unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_uint16(Extent::MutableRow &row, uint16_t value) {
            std::cerr << "Setting uint16 unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_int32(Extent::MutableRow &row, int32_t value) {
            std::cerr << "Setting int32 unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_uint32(Extent::MutableRow &row, uint32_t value) {
            std::cerr << "Setting uint32 unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_int64(Extent::MutableRow &row, int64_t value) {
            std::cerr << "Setting int64 unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_uint64(Extent::MutableRow &row, uint64_t value) {
            std::cerr << "Setting uint64 unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_text(Extent::MutableRow &row, const std::string &value) {
            std::cerr << "Setting string unsupported for this field." << std::endl;
            throw TypeError();
        }

        virtual void set_binary(Extent::MutableRow &row, const std::vector<char> &val) {
            std::cerr << "Setting binary type unsupported for this field." << std::endl;
            throw TypeError();
        }

        // set this field from the value of another field
        virtual void set_field(Extent::MutableRow &row,
                               const Extent::Row &input_row,
                               const Field &field) {
            std::cerr << "Setting from a Field unsupported for this field." << std::endl;
            throw TypeError();
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
                throw TypeError("Invalid NumberField type");
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
                throw TypeError("Invalid NumberField type");
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

}
