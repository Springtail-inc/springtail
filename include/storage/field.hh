#pragma once

#include <iostream>
#include <type_traits>
#include <cassert>

#include <storage/extent.hh>
#include <storage/schema_type.hh>

namespace springtail {

    /**
     * A three-valued object that can be either TRUE, FALSE or NULL.
     */
    class Ternary {
    private:
        static const uint8_t NULL_MASK = 0x2;
        static const uint8_t VALUE_MASK = 0x1;

    private:
        /** Hold's a three-way value: 0x2 as NULL, 0x1 as TRUE, 0x0 as FALSE. */
        uint8_t _value;

    public:
        /** Copy constructor */
        Ternary(const Ternary &result)
            : _value(result._value)
        { }

        /** Default constructor creates a null result. */
        Ternary()
            : _value(0x2)
        { }

        /** Constructor to create a TRUE or FALSE result. */
        Ternary(bool value)
            : _value(value)
        { }

        /** Checks for NULL. */
        bool is_null() {
            return ((_value & NULL_MASK) > 0);
        }

        /** Checks for TRUE / FALSE. */
        bool get_bool() {
            return ((_value & VALUE_MASK) > 0);
        }
    };

    /**
     * The Field class defines the interface for a generic column value.  That value may be extracted from
     * an extent row, it could be a constant, or it could be some operator on top of other fields.
     */
    class Field {
    public:
        virtual ~Field() { }

        virtual SchemaType get_type() const = 0;

        /** Returns true if the field is nullable.  By default fields are not nullable. */
        virtual bool is_nullable() const {
            return false;
        }

        // functions to read values

        virtual bool is_null(const Extent::Row &row) const {
            return false;
        }

        virtual bool get_bool(const Extent::Row &row) const {
            std::cerr << "Getting bool type unsupported for this field." << std::endl;
            throw TypeError();
        }

        /** Returns a Ternary based on the results of is_null() and get_bool(). */
        virtual Ternary get_ternary(const Extent::Row &row) const {
            if (this->is_null(row)) {
                return Ternary();
            }
            return Ternary(this->get_bool(row));
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

        virtual bool less_than(const Extent::Row &lhs_row,
                               std::shared_ptr<Field> rhs,
                               const Extent::Row &rhs_row,
                               bool nulls_last=true) const = 0;
    };

    /** Pointer typedef for Field. */
    typedef std::shared_ptr<Field> FieldPtr;


    /**
     * The MutableField class defines an accessor object for a given column of an extent.  It stores
     * the position of the column within the extent's fixed data and then interprets it based on the
     * type of the column.
     *
     * The data of a column in an extent is extracted by passing an Extent::Row to the
     * MutableField's approrpiate get_* function.  Calling the wrong function for a column of a
     * given type will result in an exception being thrown, so the caller must ensure that the type
     * is compatible with the get_* function being called.
     *
     * MutableField also provies a set of set_* functions to modify the values of an extent row.
     */
    class MutableField : public Field {
    protected:
        uint32_t _offset; ///< The offset into the fixed data for the row.

    protected:
        // used by Nullable fields because of virtual inheritance
        MutableField()
        { }

    public:
        MutableField(uint32_t offset)
            : _offset(offset)
        { }

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
                               FieldPtr field) {
            std::cerr << "Setting from a Field unsupported for this field." << std::endl;
            throw TypeError();
        }
    };

    /** Pointer typedef for MutableField. */
    typedef std::shared_ptr<MutableField> MutableFieldPtr;

    class NullableField : virtual public MutableField {
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

        /** NullableField and its child classes are nullable. */
        virtual bool is_nullable() const {
            return true;
        }

        bool is_null(const Extent::Row &row) const {
            const char * const fixed = row.data() + _null_offset;
            return ((*reinterpret_cast<const uint8_t * const>(fixed) & _null_mask) > 0);
        }

        void set_null(Extent::MutableRow &row, bool is_null) {
            char * const fixed = row.data() + _null_offset;
            char mask = static_cast<char>(-1) & _null_mask;
            if (is_null) {
                *fixed |= mask;
            } else {
                *fixed &= ~mask;
            }
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       FieldPtr field)
        {
            this->set_null(row, field->is_null(input_row));
        }
    };

    class BoolField : virtual public MutableField {
    private:
        char _bit_mask; ///< The bitmask for the bit of this boolean

    public:
        BoolField(uint32_t offset, uint8_t bool_bit)
            : MutableField(offset)
        {
            assert(bool_bit < 8);
            _bit_mask = static_cast<char>(1) << bool_bit;
        }

        virtual SchemaType get_type() const {
            return SchemaType::BOOLEAN;
        }

        bool get_bool(const Extent::Row &row) const {
            const char * const fixed = row.data() + _offset;
            return ((*reinterpret_cast<const uint8_t * const>(fixed) & _bit_mask) > 0);
        }

        virtual void set_bool(Extent::MutableRow &row, bool val) {
            char *fixed = row.data() + _offset;
            char mask = (static_cast<char>(-1) & _bit_mask);
            if (val) {
                *fixed |= mask;
            } else {
                *fixed &= ~mask;
            }
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       FieldPtr field)
        {
            this->set_bool(row, field->get_bool(input_row));
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            // we know this field cannot be null
            if (rhs->is_null(rhs_row)) {
                return nulls_last;
            }

            // compare boolean
            return (this->get_bool(lhs_row) < rhs->get_bool(rhs_row));
        }
    };

    class NullableBoolField : public BoolField, public NullableField {
    public:
        NullableBoolField(uint32_t offset, uint8_t bool_bit,
                          uint32_t null_offset, uint8_t null_bit)
            : MutableField(offset),
              BoolField(offset, bool_bit),
              NullableField(null_offset, null_bit)
        { }

        virtual SchemaType get_type() const {
            return BoolField::get_type();
        }


        void set_bool(Extent::MutableRow &row, bool val) {
            BoolField::set_bool(row, val);
            NullableField::set_null(row, false);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       FieldPtr field)
        {
            BoolField::set_field(row, input_row, field);
            NullableField::set_field(row, input_row, field);
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            // check if this is null
            if (this->is_null(lhs_row)) {
                if (rhs->is_null(rhs_row)) {
                    return false; // both null
                } else {
                    return !nulls_last; // lhs is null, rhs is not
                }
            }

            // compare boolean
            return BoolField::less_than(lhs_row, rhs, rhs_row, nulls_last);
        }
    };

    /** Parent field type used for all numeric types (integers and floats) */
    template <class T>
    class NumberField : virtual public MutableField {
    protected:
        size_t _fixed_size;

    protected:
        T _get_number(const Extent::Row &row) const {
            T data;
            std::copy_n(row.data() + _offset, _fixed_size, reinterpret_cast<char *>(&data));
            return data;
        }

        void _set_number(Extent::MutableRow &row, T val) {
            char *fixed = row.data() + _offset;
            std::copy_n(reinterpret_cast<char *>(&val), _fixed_size, fixed);
        }

    public:
        NumberField(uint32_t offset)
            : MutableField(offset),
              _fixed_size(sizeof(T))
        {
            static_assert(std::is_arithmetic<T>::value,
                          "NumberField must be of an arithmetic type");
        }

        SchemaType get_type() const {
            if constexpr(std::is_same_v<T, uint64_t>) {
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
            return static_cast<int16_t>(_get_number(row));
        }

        uint16_t get_uint16(const Extent::Row &row) const {
            return static_cast<uint16_t>(_get_number(row));
        }

        int32_t get_int32(const Extent::Row &row) const {
            return static_cast<int32_t>(_get_number(row));
        }

        uint32_t get_uint32(const Extent::Row &row) const {
            return static_cast<uint32_t>(_get_number(row));
        }

        int64_t get_int64(const Extent::Row &row) const {
            return static_cast<int64_t>(_get_number(row));
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
                       FieldPtr field)
        {
            if constexpr(std::is_same<T, uint64_t>::value) {
                this->set_uint64(row, field->get_uint64(input_row));
            } else if constexpr(std::is_same_v<T, int64_t>) {
                this->set_int64(row, field->get_int64(input_row));
            } else if constexpr(std::is_same_v<T, uint32_t>) {
                this->set_uint32(row, field->get_uint32(input_row));
            } else if constexpr(std::is_same_v<T, int32_t>) {
                this->set_int32(row, field->get_int32(input_row));
            } else if constexpr(std::is_same_v<T, uint16_t>) {
                this->set_uint16(row, field->get_uint16(input_row));
            } else if constexpr(std::is_same_v<T, int16_t>) {
                this->set_int16(row, field->get_int16(input_row));
            } else if constexpr(std::is_same_v<T, uint8_t>) {
                this->set_uint8(row, field->get_uint8(input_row));
            } else if constexpr(std::is_same_v<T, int8_t>) {
                this->set_int8(row, field->get_int8(input_row));
            } else if constexpr(std::is_same_v<T, double>) {
                this->set_float64(row, field->get_float64(input_row));
            } else if constexpr(std::is_same_v<T, float>) {
                this->set_float32(row, field->get_float32(input_row));
            } else {
                throw TypeError("Invalid NumberField type");
            }
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            // we know this field cannot be null
            if (rhs->is_null(rhs_row)) {
                return nulls_last;
            }

            // compare based on this field's type
            if constexpr(std::is_same_v<T, uint64_t>) {
                return (this->get_uint64(lhs_row) < rhs->get_uint64(rhs_row));
            } else if constexpr(std::is_same_v<T, int64_t>) {
                return (this->get_int64(lhs_row) < rhs->get_int64(rhs_row));
            } else if constexpr(std::is_same_v<T, uint32_t>) {
                return (this->get_uint32(lhs_row) < rhs->get_uint32(rhs_row));
            } else if constexpr(std::is_same_v<T, int32_t>) {
                return (this->get_int32(lhs_row) < rhs->get_int32(rhs_row));
            } else if constexpr(std::is_same_v<T, uint16_t>) {
                return (this->get_uint16(lhs_row) < rhs->get_uint16(rhs_row));
            } else if constexpr(std::is_same_v<T, int16_t>) {
                return (this->get_int16(lhs_row) < rhs->get_int16(rhs_row));
            } else if constexpr(std::is_same_v<T, uint8_t>) {
                return (this->get_uint8(lhs_row) < rhs->get_uint8(rhs_row));
            } else if constexpr(std::is_same_v<T, int8_t>) {
                return (this->get_int8(lhs_row) < rhs->get_int8(rhs_row));
            } else if constexpr(std::is_same_v<T, double>) {
                return (this->get_float64(lhs_row) < rhs->get_float64(rhs_row));
            } else if constexpr(std::is_same_v<T, float>) {
                return (this->get_float32(lhs_row) < rhs->get_float32(rhs_row));
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
            : MutableField(offset),
              NumberField<T>(offset),
              NullableField(null_offset, null_bit)
        { }

        virtual SchemaType get_type() const {
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
                       FieldPtr field)
        {
            NumberField<T>::set_field(row, input_row, field);
            NullableField::set_field(row, input_row, field);
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            // check if this is null first
            if (this->is_null(lhs_row)) {
                if (rhs->is_null(rhs_row)) {
                    return false; // both null
                } else {
                    return !nulls_last; // lhs is null, rhs is not
                }
            }

            return NumberField<T>::less_than(lhs_row, rhs, rhs_row, nulls_last);
        }
    };

    class TextField : virtual public MutableField {
    public:
        TextField(uint32_t offset)
            : MutableField(offset)
        { }

        SchemaType get_type() const {
            return SchemaType::TEXT;
        }

        std::string get_text(const Extent::Row &row) const {
            // get the offset into the variable data
            uint32_t offset;
            std::copy_n(row.data() + _offset, sizeof(uint32_t), reinterpret_cast<char *>(&offset));

            // retrieve the string from the variable data
            return row.extent->get_text(offset);
        }

        void set_text(Extent::MutableRow &row, const std::string &value) {
            // store the string into the variable data
            uint32_t offset = row.set_text(value);

            // store the offset into the fixed data
            char *fixed = row.data() + _offset;
            std::copy_n(reinterpret_cast<char *>(&offset), sizeof(uint32_t), fixed);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       FieldPtr field)
        {
            // XXX if the field is a TEXT or BINARY then we want to do a direct copy

            // otherwise we can just call get_text()
            this->set_text(row, field->get_text(input_row));
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            // we know this field cannot be null
            if (rhs->is_null(rhs_row)) {
                return nulls_last;
            }

            // compare boolean
            return (this->get_text(lhs_row) < rhs->get_text(rhs_row));
        }
    };

    class NullableTextField : public TextField, public NullableField {
    public:
        NullableTextField(uint32_t offset, uint32_t null_offset, uint8_t null_bit)
            : MutableField(offset),
              TextField(offset),
              NullableField(null_offset, null_bit)
        { }

        virtual SchemaType get_type() const {
            return TextField::get_type();
        }

        void set_text(Extent::MutableRow &row, const std::string &value) {
            TextField::set_text(row, value);
            NullableField::set_null(row, false);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       FieldPtr field)
        {
            TextField::set_field(row, input_row, field);
            NullableField::set_field(row, input_row, field);
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            // check if this is null
            if (this->is_null(lhs_row)) {
                if (rhs->is_null(rhs_row)) {
                    return false; // both null
                } else {
                    return !nulls_last; // lhs is null, rhs is not
                }
            }

            // compare text
            return TextField::less_than(lhs_row, rhs, rhs_row, nulls_last);
        }
    };

    class BinaryField : virtual public MutableField {
    public:
        BinaryField(uint32_t offset)
            : MutableField(offset)
        { }

        SchemaType get_type() const {
            return SchemaType::BINARY;
        }

        const std::vector<char> get_binary(const Extent::Row &row) const {
            uint32_t offset;

            // get the offset into the variable data
            std::copy_n(row.data() + _offset, sizeof(uint32_t), reinterpret_cast<char *>(&offset));

            // retrieve the binary data from the variable data
            return row.extent->get_binary(offset);
        }

        void set_binary(Extent::MutableRow &row, const std::vector<char> &val) {
            // store the data into the variable-sized data
            uint32_t offset = row.set_binary(val);

            // copy the offset into the fixed data
            char *fixed = row.data() + _offset;
            std::copy_n(reinterpret_cast<char *>(&offset), sizeof(uint32_t), fixed);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       FieldPtr field)
        {
            // XXX if the field is a TEXT or BINARY then we want to do a direct copy

            // otherwise we can just call get_binary()
            this->set_binary(row, field->get_binary(input_row));
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            throw TypeError("Can't compare binary fields -- unclear how to handle");
        }
    };

    class NullableBinaryField : public BinaryField, public NullableField {
    public:
        NullableBinaryField(uint32_t offset, uint32_t null_offset, uint8_t null_bit)
            : MutableField(offset),
              BinaryField(offset),
              NullableField(null_offset, null_bit)
        { }

        virtual SchemaType get_type() const {
            return BinaryField::get_type();
        }

        void set_binary(Extent::MutableRow &row, const std::vector<char> &val) {
            BinaryField::set_binary(row, val);
            NullableField::set_null(row, false);
        }

        void set_field(Extent::MutableRow &row,
                       const Extent::Row &input_row,
                       FieldPtr field)
        {
            BinaryField::set_field(row, input_row, field);
            NullableField::set_field(row, input_row, field);
        }
    };

    /**
     * A constant value that is represented as a Field for use in conditionals and joins.
     */
    template <class T>
    class ConstField : public Field {
    private:
        T _value;

    public:
        ConstField(T value)
            : _value(value)
        { }

        virtual SchemaType get_type() const {
            if constexpr(std::is_same_v<T, bool>) {
                return SchemaType::BOOLEAN;
            } else if constexpr(std::is_same_v<T, uint64_t>) {
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
            } else if constexpr(std::is_same_v<T, std::string>) {
                return SchemaType::TEXT;
            } else if constexpr(std::is_same_v<T, std::vector<char>>) {
                return SchemaType::BINARY;
            }

            throw TypeError();
        }

        bool get_bool(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, bool>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        int8_t get_int8(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, int8_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        uint8_t get_uint8(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, uint8_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        int16_t get_int16(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, int16_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        uint16_t get_uint16(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, uint16_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        int32_t get_int32(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, int32_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        uint32_t get_uint32(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, uint32_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        int64_t get_int64(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, int64_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        uint64_t get_uint64(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, uint64_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        float get_float32(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, float>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        double get_float64(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, double>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        std::string get_text(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, std::string>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        const std::vector<char> get_binary(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, std::vector<char>>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            // if the rhs is null, then it is always either larger or smaller than a constant value
            if (rhs->is_null(rhs_row)) {
                return nulls_last;
            }

            // return based on the comparison for the type of this constant
            if constexpr(std::is_same_v<T, bool>) {
                return (this->get_bool(lhs_row) < rhs->get_bool(rhs_row));

            } else if constexpr(std::is_same_v<T, int8_t>) {
                return (this->get_int8(lhs_row) < rhs->get_int8(rhs_row));

            } else if constexpr(std::is_same_v<T, uint8_t>) {
                return (this->get_uint8(lhs_row) < rhs->get_uint8(rhs_row));

            } else if constexpr(std::is_same_v<T, int16_t>) {
                return (this->get_int16(lhs_row) < rhs->get_int16(rhs_row));

            } else if constexpr(std::is_same_v<T, uint16_t>) {
                return (this->get_uint16(lhs_row) < rhs->get_uint16(rhs_row));

            } else if constexpr(std::is_same_v<T, int32_t>) {
                return (this->get_int32(lhs_row) < rhs->get_int32(rhs_row));

            } else if constexpr(std::is_same_v<T, uint32_t>) {
                return (this->get_uint32(lhs_row) < rhs->get_uint32(rhs_row));

            } else if constexpr(std::is_same_v<T, int64_t>) {
                return (this->get_int64(lhs_row) < rhs->get_int64(rhs_row));

            } else if constexpr(std::is_same_v<T, uint64_t>) {
                return (this->get_uint64(lhs_row) < rhs->get_uint64(rhs_row));

            } else if constexpr(std::is_same_v<T, float>) {
                return (this->get_float32(lhs_row) < rhs->get_float32(rhs_row));

            } else if constexpr(std::is_same_v<T, double>) {
                return (this->get_float64(lhs_row) < rhs->get_float64(rhs_row));

            } else if constexpr(std::is_same_v<T, std::string>) {
                return (this->get_text(lhs_row) < rhs->get_text(rhs_row));

            } else if constexpr(std::is_same_v<T, std::vector<char>>) {
                return (this->get_binary(lhs_row) < rhs->get_binary(rhs_row));

            } else {
                throw TypeError();
            }
        }
    };

    /**
     * A value that is always null represented as a Field for use in conditionals and joins.
     */
    class ConstNullField : public Field {
    private:
        SchemaType _type;

    public:
        ConstNullField(SchemaType type)
            : _type(type)
        { }

        virtual SchemaType get_type() const {
            return _type;
        }

        virtual bool is_nullable() const {
            return true;
        }

        bool is_null(const Extent::Row &row) const {
            return true;
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            if (rhs->is_null(rhs_row)) {
                return false;
            }

            return !nulls_last;
        }
    };

    /**
     * A field that wraps another field to return a default value if the underlying value is null.
     */
    template <class T>
    class NullWrapperField : public Field {
    private:
        FieldPtr _field;
        T _default;

    public:
        NullWrapperField(FieldPtr field, const T &default_value)
            : _field(field), _default(default_value)
        { }

        NullWrapperField(FieldPtr field, T &&default_value)
            : _field(field), _default(default_value)
        { }

        virtual SchemaType get_type() const {
            return _field->get_type();
        }

        virtual bool is_nullable() const {
            return false;
        }

        bool is_null(const Extent::Row &row) const {
            return false;
        }

        bool get_bool(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, bool>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_bool(row);
            } else {
                throw TypeError();
            }
        }

        int8_t get_int8(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, int8_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_int8(row);
            } else {
                throw TypeError();
            }
        }

        uint8_t get_uint8(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, uint8_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_uint8(row);
            } else {
                throw TypeError();
            }
        }

        int16_t get_int16(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, int16_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_int16(row);
            } else {
                throw TypeError();
            }
        }

        uint16_t get_uint16(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, uint16_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_uint16(row);
            } else {
                throw TypeError();
            }
        }

        int32_t get_int32(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, int32_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_int32(row);
            } else {
                throw TypeError();
            }
        }

        uint32_t get_uint32(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, uint32_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_uint32(row);
            } else {
                throw TypeError();
            }
        }

        int64_t get_int64(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, int64_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_int64(row);
            } else {
                throw TypeError();
            }
        }

        uint64_t get_uint64(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, uint64_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_uint64(row);
            } else {
                throw TypeError();
            }
        }

        float get_float32(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, float>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_float32(row);
            } else {
                throw TypeError();
            }
        }

        double get_float64(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, double>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_float64(row);
            } else {
                throw TypeError();
            }
        }

        std::string get_text(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, std::string>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_text(row);
            } else {
                throw TypeError();
            }
        }

        const std::vector<char> get_binary(const Extent::Row &row) const {
            if constexpr(std::is_same_v<T, std::vector<char>>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_binary(row);
            } else {
                throw TypeError();
            }
        }        

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const
        {
            // if the rhs is null, then it is always either larger or smaller than a constant value
            if (rhs->is_null(rhs_row)) {
                return nulls_last;
            }

            // return based on the comparison for the type of this constant
            if constexpr(std::is_same_v<T, bool>) {
                return (this->get_bool(lhs_row) < rhs->get_bool(rhs_row));

            } else if constexpr(std::is_same_v<T, int8_t>) {
                return (this->get_int8(lhs_row) < rhs->get_int8(rhs_row));

            } else if constexpr(std::is_same_v<T, uint8_t>) {
                return (this->get_uint8(lhs_row) < rhs->get_uint8(rhs_row));

            } else if constexpr(std::is_same_v<T, int16_t>) {
                return (this->get_int16(lhs_row) < rhs->get_int16(rhs_row));

            } else if constexpr(std::is_same_v<T, uint16_t>) {
                return (this->get_uint16(lhs_row) < rhs->get_uint16(rhs_row));

            } else if constexpr(std::is_same_v<T, int32_t>) {
                return (this->get_int32(lhs_row) < rhs->get_int32(rhs_row));

            } else if constexpr(std::is_same_v<T, uint32_t>) {
                return (this->get_uint32(lhs_row) < rhs->get_uint32(rhs_row));

            } else if constexpr(std::is_same_v<T, int64_t>) {
                return (this->get_int64(lhs_row) < rhs->get_int64(rhs_row));

            } else if constexpr(std::is_same_v<T, uint64_t>) {
                return (this->get_uint64(lhs_row) < rhs->get_uint64(rhs_row));

            } else if constexpr(std::is_same_v<T, float>) {
                return (this->get_float32(lhs_row) < rhs->get_float32(rhs_row));

            } else if constexpr(std::is_same_v<T, double>) {
                return (this->get_float64(lhs_row) < rhs->get_float64(rhs_row));

            } else if constexpr(std::is_same_v<T, std::string>) {
                return (this->get_text(lhs_row) < rhs->get_text(rhs_row));

            } else if constexpr(std::is_same_v<T, std::vector<char>>) {
                return (this->get_binary(lhs_row) < rhs->get_binary(rhs_row));

            } else {
                throw TypeError();
            }
        }
    };

    /**
     * An array of values, encapsulated such that they can be compared even when coming from
     * different tables or different rows within a table, or are just fixed values (e.g., ValueTuple).
     */
    class Tuple {
    public:
        virtual ~Tuple() { }

        virtual std::size_t size() const = 0;
        virtual std::shared_ptr<Field> field(int idx) const = 0;
        virtual Extent::Row row() const = 0;

        bool less_than(std::shared_ptr<Tuple> rhs, bool nulls_last=true) const {
            // check the tuple lengths and types using assert()
            // we assume correct usage in production
            assert(this->size() == rhs->size());
            for (int i = 0; i < this->size(); i++) {
                assert(this->field(i)->get_type() == rhs->field(i)->get_type());
            }

            // XXX switch everything to compare()?
            for (int i = 0; i < this->size(); i++) {
                if (this->field(i)->less_than(this->row(), rhs->field(i), rhs->row(), nulls_last)) {
                    return true;
                }
                if (rhs->field(i)->less_than(rhs->row(), this->field(i), this->row(), nulls_last)) {
                    return false;
                }
            }

            // all values are equal, so not less than
            return false;
        }

        void
        print() {
            std::string txt;
            for (int i = 0; i < this->size(); i++) {
                FieldPtr field = this->field(i);
                switch (field->get_type()) {
                case (SchemaType::TEXT):
                    std::cout << field->get_text(this->row()) << std::endl;
                    break;
                case (SchemaType::UINT64):
                    std::cout << field->get_uint64(this->row()) << std::endl;
                    break;
                default:
                    std::cout << "Unhandled type" << std::endl;
                }
            }
        }
    };
    typedef std::shared_ptr<Tuple> TuplePtr;

    /**
     * Interface for a mutable Tuple that can have it's value set by another Tuple.
     */
    class MutableTuple : public Tuple {
    public:
        virtual MutableFieldPtr mutable_field(int idx) const = 0;
        virtual Extent::MutableRow mutable_row() const = 0;

    public:
        /** Copy the data from another Tuple into this Tuple. */
        void assign(TuplePtr other)
        {
            for (int i = 0; i < size(); i++) {
                Extent::MutableRow row = mutable_row();
                mutable_field(i)->set_field(row, other->row(), other->field(i));
            }
        }
    };
    typedef std::shared_ptr<MutableTuple> MutableTuplePtr;

    /**
     * Represents an array of fields.  Can be bound to a row to generate a Tuple.
     */
    class FieldArray {
    public:
        virtual ~FieldArray() { }
        virtual TuplePtr bind(const Extent::Row &row) = 0;
        virtual FieldPtr operator[](std::size_t idx) const = 0;
        virtual uint32_t size() const = 0;
    };
    typedef std::shared_ptr<FieldArray> FieldArrayPtr;

    /**
     * A read-only field array.
     */
    class ReadFieldArray : public FieldArray, public std::enable_shared_from_this<ReadFieldArray> {
    private:
        std::vector<FieldPtr> _fields;

    public:
        ReadFieldArray() = default;
        ReadFieldArray(const std::vector<FieldPtr> &fields)
            : _fields(fields)
        { }
        ReadFieldArray(std::vector<FieldPtr> &&fields)
            : _fields(fields)
        { }
        ReadFieldArray(ReadFieldArray &&set)
            : _fields(std::move(set._fields))
        { }

        TuplePtr bind(const Extent::Row &row) override;

        FieldPtr operator[](std::size_t idx) const override {
            return _fields[idx];
        }

        uint32_t size() const override {
            return _fields.size();
        }
    };
    typedef std::shared_ptr<ReadFieldArray> ReadFieldArrayPtr;

    /**
     * A specialized array of fields that can generate a MutableTuple for updating a row.
     */
    class MutableFieldArray : public FieldArray, public std::enable_shared_from_this<MutableFieldArray> {
    private:
        std::vector<MutableFieldPtr> _fields;

    public:
        MutableFieldArray() = default;
        MutableFieldArray(const std::vector<MutableFieldPtr> &fields)
            : _fields(fields)
        { }
        MutableFieldArray(std::vector<MutableFieldPtr> &&fields)
            : _fields(fields)
        { }
        MutableFieldArray(MutableFieldArray &&set)
            : _fields(std::move(set._fields))
        { }

        FieldPtr operator[](std::size_t idx) const override {
            return _fields[idx];
        }

        uint32_t size() const override {
            return _fields.size();
        }

        TuplePtr bind(const Extent::Row &row) override;

        //// functions for mutability

        MutableFieldPtr get_mutable(std::size_t idx) const {
            return _fields[idx];
        }

        MutableTuplePtr bind(const Extent::MutableRow &row);
    };
    typedef std::shared_ptr<MutableFieldArray> MutableFieldArrayPtr;

    /**
     * Implements the Tuple interface using a FieldArray and a bound Extent::Row.
     */
    class RowTuple : public Tuple {
        FieldArrayPtr _array;
        Extent::Row _row;

    public:
        RowTuple(const RowTuple &tuple) = default;
        RowTuple(RowTuple &&tuple) = default;
        RowTuple(FieldArrayPtr array, const Extent::Row &row)
            : _array(array), _row(row)
        { }

        std::size_t size() const {
            return _array->size();
        }

        FieldPtr field(int idx) const {
            return (*_array)[idx];
        }

        Extent::Row row() const {
            return _row;
        }
    };

    /**
     * Implements the Tuple interface using a MutableFieldArray and a bound Extent::MutableRow.  Also offers 
     */
    class MutableRowTuple : public MutableTuple {
        MutableFieldArrayPtr _array;
        Extent::MutableRow _row;

    public:
        MutableRowTuple(const MutableRowTuple &tuple) = default;
        MutableRowTuple(MutableRowTuple &&tuple) = default;
        MutableRowTuple(MutableFieldArrayPtr array, const Extent::MutableRow &row)
            : _array(array), _row(row)
        { }

        std::size_t size() const {
            return _array->size();
        }

        FieldPtr field(int idx) const {
            return (*_array)[idx];
        }

        Extent::Row row() const {
            return _row;
        }

        MutableFieldPtr mutable_field(int idx) const {
            return _array->get_mutable(idx);
        }

        Extent::MutableRow mutable_row() const {
            return _row;
        }
    };

    class ValueTuple : public Tuple {
        /** An array of ConstField objects. */
        std::vector<std::shared_ptr<Field>> _fields;

    public:
        ValueTuple()
        { }

        ValueTuple(TuplePtr tuple)
        {
            // copy the data from the tuple into const fields
            for (int i = 0; i < tuple->size(); i++) {
                std::shared_ptr<Field> field = tuple->field(i);

                if (field->is_null(tuple->row())) {
                    _fields.push_back(std::make_shared<ConstNullField>(field->get_type()));
                } else {
                    switch(tuple->field(i)->get_type()) {
                    case(SchemaType::TEXT):
                        _fields.push_back(std::make_shared<ConstField<std::string>>(field->get_text(tuple->row())));
                        break;
                    case(SchemaType::UINT64):
                        _fields.push_back(std::make_shared<ConstField<uint64_t>>(field->get_uint64(tuple->row())));
                        break;
                    case(SchemaType::INT64):
                        _fields.push_back(std::make_shared<ConstField<int64_t>>(field->get_int64(tuple->row())));
                        break;
                    case(SchemaType::UINT32):
                        _fields.push_back(std::make_shared<ConstField<uint32_t>>(field->get_uint32(tuple->row())));
                        break;
                    case(SchemaType::INT32):
                        _fields.push_back(std::make_shared<ConstField<int32_t>>(field->get_int32(tuple->row())));
                        break;
                    case(SchemaType::UINT16):
                        _fields.push_back(std::make_shared<ConstField<uint16_t>>(field->get_uint16(tuple->row())));
                        break;
                    case(SchemaType::INT16):
                        _fields.push_back(std::make_shared<ConstField<int16_t>>(field->get_int16(tuple->row())));
                        break;
                    case(SchemaType::UINT8):
                        _fields.push_back(std::make_shared<ConstField<uint8_t>>(field->get_uint8(tuple->row())));
                        break;
                    case(SchemaType::INT8):
                        _fields.push_back(std::make_shared<ConstField<int8_t>>(field->get_int8(tuple->row())));
                        break;
                    case(SchemaType::BOOLEAN):
                        _fields.push_back(std::make_shared<ConstField<bool>>(field->get_bool(tuple->row())));
                        break;
                    case(SchemaType::FLOAT64):
                        _fields.push_back(std::make_shared<ConstField<double>>(field->get_float64(tuple->row())));
                        break;
                    case(SchemaType::FLOAT32):
                        _fields.push_back(std::make_shared<ConstField<float>>(field->get_float32(tuple->row())));
                        break;
                    case(SchemaType::BINARY):
                        _fields.push_back(std::make_shared<ConstField<std::vector<char>>>(field->get_binary(tuple->row())));
                        break;
                    default:
                        throw TypeError();
                    }
                }
            }
        }

        std::size_t size() const {
            return _fields.size();
        }

        std::shared_ptr<Field> field(int idx) const {
            return _fields[idx];
        }

        Extent::Row row() const {
            // note: only usable with the ConstField objects from this ValueTuple
            return Extent::Row(nullptr, 0);
        }
    };
    typedef std::shared_ptr<ValueTuple> ValueTuplePtr;
}
