#pragma once

#include <type_traits>
#include <bit>

#include <absl/log/check.h>
#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <storage/extent.hh>
#include <storage/numeric_utils.hh>
#include <storage/schema_type.hh>
#include <storage/schema.hh>

namespace springtail {

    /**
     * The Field class defines the interface for storing / retrieving column values from an
     * underlying data row.  That value may be extracted from an extent row, it could be a constant,
     * or it could be some operator on top of other fields.
     */
    class Field {
    public:
        virtual ~Field() = default;

        virtual SchemaType get_type() const = 0;

        virtual bool can_be_undefined() const {
            return false;
        }

        /** Returns true if the field is nullable.  By default fields are not nullable. */
        virtual bool can_be_null() const {
            return false;
        }

        // functions to read values

        virtual bool is_undefined(const void *row) const {
            return false;
        }

        virtual bool is_null(const void *row) const {
            return false;
        }

        virtual bool get_bool(const void *row) const {
            throw TypeError("Getting bool type unsupported for this field.");
        }

        virtual int8_t get_int8(const void *row) const {
            throw TypeError("Getting int8 type unsupported for this field.");
        }

        virtual uint8_t get_uint8(const void *row) const {
            throw TypeError("Getting uint8 type unsupported for this field.");
        }

        virtual int16_t get_int16(const void *row) const {
            throw TypeError("Getting int16 type unsupported for this field.");
        }

        virtual uint16_t get_uint16(const void *row) const {
            throw TypeError("Getting uint16 type unsupported for this field.");
        }

        virtual int32_t get_int32(const void *row) const {
            throw TypeError("Getting int32 type unsupported for this field.");
        }

        virtual uint32_t get_uint32(const void *row) const {
            throw TypeError("Getting uint32 type unsupported for this field.");
        }

        virtual int64_t get_int64(const void *row) const {
            throw TypeError("Getting int64 type unsupported for this field.");
        }

        virtual uint64_t get_uint64(const void *row) const {
            throw TypeError("Getting uint64 type unsupported for this field.");
        }

        virtual float get_float32(const void *row) const {
            throw TypeError("Getting float32 type unsupported for this field.");
        }

        virtual double get_float64(const void *row) const {
            throw TypeError("Getting float64 type unsupported for this field.");
        }

        virtual std::string_view get_text(const void *row) const {
            throw TypeError("Getting text type unsupported for this field.");
        }

        virtual const std::shared_ptr<numeric::NumericData> get_numeric(const void *row) const {
            throw TypeError("Getting numeric type unsupported for this field.");
        }

        virtual const std::span<const char> get_binary(const void *row) const {
            throw TypeError("Getting binary type unsupported for this field.");
        }

        bool
        less_than(const void *lhs_row,
                  const std::shared_ptr<Field>& rhs,
                  const void *rhs_row,
                  bool nulls_last=true) const
        {
            // types must match
            CHECK_EQ(this->get_type(), rhs->get_type());

            // cannot call less_than() on undefined fields
            CHECK(!(this->is_undefined(lhs_row) || rhs->is_undefined(rhs_row)));

            // handle nulls
            if (this->is_null(lhs_row)) {
                if (rhs->is_null(rhs_row)) {
                    return false; // both null, so equal
                } else {
                    return !nulls_last; // lhs is null, rhs is not
                }
            } else {
                if (rhs->is_null(rhs_row)) {
                    return nulls_last; // lhs is not null, rhs is null
                }
            }

            // handle values
            switch (this->get_type()) {
            case SchemaType::BOOLEAN:
                return (this->get_bool(lhs_row) < rhs->get_bool(rhs_row));

            case SchemaType::UINT8:
                return (this->get_uint8(lhs_row) < rhs->get_uint8(rhs_row));

            case SchemaType::INT8:
                return (this->get_int8(lhs_row) < rhs->get_int8(rhs_row));

            case SchemaType::UINT16:
                return (this->get_uint16(lhs_row) < rhs->get_uint16(rhs_row));

            case SchemaType::INT16:
                return (this->get_int16(lhs_row) < rhs->get_int16(rhs_row));

            case SchemaType::UINT32:
                return (this->get_uint32(lhs_row) < rhs->get_uint32(rhs_row));

            case SchemaType::INT32:
                return (this->get_int32(lhs_row) < rhs->get_int32(rhs_row));

            case SchemaType::UINT64:
                return (this->get_uint64(lhs_row) < rhs->get_uint64(rhs_row));

            case SchemaType::INT64:
                return (this->get_int64(lhs_row) < rhs->get_int64(rhs_row));

            case SchemaType::FLOAT32:
                return (this->get_float32(lhs_row) < rhs->get_float32(rhs_row));

            case SchemaType::FLOAT64:
                return (this->get_float64(lhs_row) < rhs->get_float64(rhs_row));

            case SchemaType::TEXT:
                return (this->get_text(lhs_row) < rhs->get_text(rhs_row));

            case SchemaType::NUMERIC: {
                numeric::Numeric lhs_numeric = this->get_numeric(lhs_row).get();
                numeric::Numeric rhs_numeric = rhs->get_numeric(rhs_row).get();
                return (numeric::NumericData::cmp(lhs_numeric, rhs_numeric) == -1);
            }

            case SchemaType::BINARY: {
                // retrieve the binary data
                auto lhval = this->get_binary(lhs_row);
                auto rhval = rhs->get_binary(rhs_row);

                // perform a byte-wise comparison
                auto min = std::min(lhval.size(), rhval.size());
                auto cmp = std::memcmp(lhval.data(), rhval.data(), min);

                // check the result
                if (cmp < 0) {
                    return true;
                } else if (cmp > 0) {
                    return false;
                }

                // if equal, check the sizes
                if (lhval.size() < rhval.size()) {
                    return true;
                }
                return false;
            }

            default:
                LOG_ERROR("Unsupported data type: {}", (int)this->get_type());
                throw TypeError();
            }
        }

        bool
        equal(const void *lhs_row,
              const std::shared_ptr<Field>& rhs,
              const void *rhs_row)
        {
            // types must match
            CHECK_EQ(this->get_type(), rhs->get_type());

            // cannot call equal() on undefined fields
            CHECK(!(this->is_undefined(lhs_row) || rhs->is_undefined(rhs_row)));

            // handle nulls
            if (this->is_null(lhs_row)) {
                if (rhs->is_null(rhs_row)) {
                    return true; // both null, so equal
                } else {
                    return false; // lhs is null, rhs is not
                }
            } else {
                if (rhs->is_null(rhs_row)) {
                    return false; // lhs is not null, rhs is null
                }
            }

            // handle values
            switch (this->get_type()) {
            case SchemaType::BOOLEAN:
                return (this->get_bool(lhs_row) == rhs->get_bool(rhs_row));

            case SchemaType::UINT8:
                return (this->get_uint8(lhs_row) == rhs->get_uint8(rhs_row));

            case SchemaType::INT8:
                return (this->get_int8(lhs_row) == rhs->get_int8(rhs_row));

            case SchemaType::UINT16:
                return (this->get_uint16(lhs_row) == rhs->get_uint16(rhs_row));

            case SchemaType::INT16:
                return (this->get_int16(lhs_row) == rhs->get_int16(rhs_row));

            case SchemaType::UINT32:
                return (this->get_uint32(lhs_row) == rhs->get_uint32(rhs_row));

            case SchemaType::INT32:
                return (this->get_int32(lhs_row) == rhs->get_int32(rhs_row));

            case SchemaType::UINT64:
                return (this->get_uint64(lhs_row) == rhs->get_uint64(rhs_row));

            case SchemaType::INT64:
                return (this->get_int64(lhs_row) == rhs->get_int64(rhs_row));

            case SchemaType::FLOAT32:
                return (this->get_float32(lhs_row) == rhs->get_float32(rhs_row));

            case SchemaType::FLOAT64:
                return (this->get_float64(lhs_row) == rhs->get_float64(rhs_row));

            case SchemaType::TEXT:
                return (this->get_text(lhs_row) == rhs->get_text(rhs_row));

            case SchemaType::NUMERIC:
                return (numeric::NumericData::cmp(this->get_numeric(lhs_row).get(), rhs->get_numeric(rhs_row).get()) == 0);

            case SchemaType::BINARY: {
                auto lhval = this->get_binary(lhs_row);
                auto rhval = rhs->get_binary(rhs_row);

                // check the sizes
                if (lhval.size() != rhval.size()) {
                    return false;
                }

                // perform a byte-wise comparison
                auto cmp = std::memcmp(lhval.data(), rhval.data(), lhval.size());
                return (cmp == 0);
            }

            default:
                LOG_ERROR("Unsupported data type: {}", (int)this->get_type());
                throw TypeError();
            }
        }
    };
    using FieldPtr = std::shared_ptr<Field>;

    /**
     * MutableField provies a set of set_* functions to modify the values of a column.  It is
     * implemented by specific Field implementations to enable modification of the underlying data.
     */
    class MutableField : public Field {
    public:
        MutableField() = default;

        // functions to set values
        virtual void set_undefined(void *row, bool is_undefined) {
            throw TypeError("Setting undefined unsupported for this field.");
        }

        virtual void set_null(void *row, bool is_null) {
            throw TypeError("Setting null unsupported for this field.");
        }

        virtual void set_bool(void *row, bool value) {
            throw TypeError("Setting bool unsupported for this field.");
        }

        virtual void set_int8(void *row, int8_t value) {
            throw TypeError("Setting int8 unsupported for this field.");
        }

        virtual void set_uint8(void *row, uint8_t value) {
            throw TypeError("Setting uint8 unsupported for this field.");
        }

        virtual void set_int16(void *row, int16_t value) {
            throw TypeError("Setting int16 unsupported for this field.");
        }

        virtual void set_uint16(void *row, uint16_t value) {
            throw TypeError("Setting uint16 unsupported for this field.");
        }

        virtual void set_int32(void *row, int32_t value) {
            throw TypeError("Setting int32 unsupported for this field.");
        }

        virtual void set_uint32(void *row, uint32_t value) {
            throw TypeError("Setting uint32 unsupported for this field.");
        }

        virtual void set_int64(void *row, int64_t value) {
            throw TypeError("Setting int64 unsupported for this field.");
        }

        virtual void set_uint64(void *row, uint64_t value) {
            throw TypeError("Setting uint64 unsupported for this field.");
        }

        virtual void set_float32(void *row, float value) {
            throw TypeError("Setting float32 unsupported for this field.");
        }

        virtual void set_float64(void *row, double value) {
            throw TypeError("Setting float64 unsupported for this field.");
        }

        virtual void set_text(void *row, const std::string_view &value) {
            throw TypeError("Setting string unsupported for this field.");
        }

        virtual void set_numeric(void *row, const std::shared_ptr<numeric::NumericData> value) {
            throw TypeError("Setting numeric type unsupported for this field.");
        }

        virtual void set_binary(void *row, const std::span<const char> &val) {
            throw TypeError("Setting binary type unsupported for this field.");
        }

        // set this field from the value of another field
        void set_field(void *lhs, FieldPtr field, const void *rhs) {
            // field types must match
            DCHECK_EQ(this->get_type(), field->get_type());

            // handle undefined data cases
            if (this->can_be_undefined()) {
                bool undefined = field->is_undefined(rhs);
                if (undefined) {
                    this->set_undefined(lhs, true);
                    return; // no more processing to do since we don't need the value
                }
            } else {
                DCHECK_EQ(field->is_undefined(rhs), false);
            }

            // handle NULL data cases
            if (this->can_be_null()) {
                bool null = field->is_null(rhs);
                if (null) {
                    this->set_null(lhs, true);
                    return; // no more processing to do since we don't need the value
                } else {
                    this->set_null(lhs, false);
                }
            } else {
                DCHECK_EQ(field->is_null(rhs), false);
            }

            // handle core data types
            switch (this->get_type()) {
            case SchemaType::BOOLEAN:
                this->set_bool(lhs, field->get_bool(rhs));
                break;

            case SchemaType::UINT8:
                this->set_uint8(lhs, field->get_uint8(rhs));
                break;

            case SchemaType::INT8:
                this->set_int8(lhs, field->get_int8(rhs));
                break;

            case SchemaType::UINT16:
                this->set_uint16(lhs, field->get_uint16(rhs));
                break;

            case SchemaType::INT16:
                this->set_int16(lhs, field->get_int16(rhs));
                break;

            case SchemaType::UINT32:
                this->set_uint32(lhs, field->get_uint32(rhs));
                break;

            case SchemaType::INT32:
                this->set_int32(lhs, field->get_int32(rhs));
                break;

            case SchemaType::UINT64:
                this->set_uint64(lhs, field->get_uint64(rhs));
                break;

            case SchemaType::INT64:
                this->set_int64(lhs, field->get_int64(rhs));
                break;

            case SchemaType::FLOAT32:
                this->set_float32(lhs, field->get_float32(rhs));
                break;

            case SchemaType::FLOAT64:
                this->set_float64(lhs, field->get_float64(rhs));
                break;

            case SchemaType::TEXT:
                this->set_text(lhs, field->get_text(rhs));
                break;

            case SchemaType::NUMERIC:
                this->set_numeric(lhs, field->get_numeric(rhs));
                break;

            case SchemaType::BINARY:
                this->set_binary(lhs, field->get_binary(rhs));
                break;

            default:
                LOG_ERROR("Unsupported data type: {}", (int)this->get_type());
                throw TypeError();
            }
        }
    };

    /** Pointer typedef for MutableField. */
    using MutableFieldPtr = std::shared_ptr<MutableField>;

    /**
     * Field on top of an Extent::Row.
     */
    class ExtentField : public MutableField {
    public:
        ExtentField(SchemaType type, uint32_t offset)
            : _type(type),
              _can_null(false),
              _can_undefined(false),
              _offset(offset)
        { }

        ExtentField(SchemaType type, uint32_t offset, uint8_t bool_bit)
            : _type(type),
              _can_null(false),
              _can_undefined(false),
              _offset(offset),
              _bool_bitmask(static_cast<char>(1) << bool_bit)
        { }

        void
        allow_null(uint32_t null_offset, uint8_t null_bit)
        {
            CHECK(null_bit < 8);

            _can_null = true;
            _null_offset = null_offset;
            _null_bitmask = (static_cast<char>(1) << null_bit);
        }

        void
        allow_undefined(uint32_t undefined_offset, uint8_t undefined_bit)
        {
            CHECK(undefined_bit < 8);

            _can_undefined = true;
            _undefined_offset = undefined_offset;
            _undefined_bitmask = (static_cast<char>(1) << undefined_bit);
        }

        SchemaType get_type() const override {
            return _type;
        }

        bool can_be_undefined() const override {
            return _can_undefined;
        }

        bool can_be_null() const override {
            return _can_null;
        }

        bool is_null(const void *row) const override {
            // if we can't set undefined, then always has a value
            if (!_can_null) {
                return false;
            }

            // return the null bit
            auto e_row = reinterpret_cast<const Extent::Row *>(row);
            return _get_bit(*e_row, _null_offset, _null_bitmask);
        }

        bool is_undefined(const void *row) const override {
            // if we can't set undefined, then always has a value
            if (!_can_undefined) {
                return false;
            }

            // return the undefined bit
            auto e_row = reinterpret_cast<const Extent::Row *>(row);
            return _get_bit(*e_row, _undefined_offset, _undefined_bitmask);
        }

        bool get_bool(const void *row) const override {
            // must be boolean type
            DCHECK_EQ(_type, SchemaType::BOOLEAN);

            auto e_row = reinterpret_cast<const Extent::Row *>(row);
            return _get_bit(*e_row, _offset, _bool_bitmask);
        }

        template <typename T>
        inline T get_value(const void* row) const {
            T result;
            std::memcpy(&result,
                    reinterpret_cast<const Extent::Row *>(row)->data() + _offset,
                    sizeof(T));
            return result;
        }

        int8_t get_int8(const void *row) const override {
            // must be int8 type
            DCHECK_EQ(_type, SchemaType::INT8);
            return *reinterpret_cast<const int8_t *>(
                    reinterpret_cast<const Extent::Row *>(row)->data() + _offset);
        }

        uint8_t get_uint8(const void *row) const override {
            // must be uint8 type
            DCHECK_EQ(_type, SchemaType::UINT8);
            return *reinterpret_cast<const uint8_t *>(
                    reinterpret_cast<const Extent::Row *>(row)->data() + _offset);
        }

        int16_t get_int16(const void *row) const override {
            // must be int16 type
            DCHECK_EQ(_type, SchemaType::INT16);
            return get_value<int16_t>(row);
        }

        uint16_t get_uint16(const void *row) const override {
            // must be uint16 type
            DCHECK_EQ(_type, SchemaType::UINT16);
            return get_value<uint16_t>(row);
        }

        int32_t get_int32(const void *row) const override {
            // must be int32 type
            DCHECK_EQ(_type, SchemaType::INT32);
            return get_value<int32_t>(row);
        }

        uint32_t get_uint32(const void *row) const override {
            // must be uint32 type
            DCHECK_EQ(_type, SchemaType::UINT32);
            return get_value<uint32_t>(row);
        }

        int64_t get_int64(const void *row) const override {
            // must be int64 type
            DCHECK_EQ(_type, SchemaType::INT64);
            return get_value<int64_t>(row);
        }

        uint64_t get_uint64(const void *row) const override {
            // must be uint64 type
            DCHECK_EQ(_type, SchemaType::UINT64);
            return get_value<uint64_t>(row);
        }

        float get_float32(const void *row) const override {
            // must be float32 type
            DCHECK_EQ(_type, SchemaType::FLOAT32);
            return get_value<float>(row);
        }

        double get_float64(const void *row) const override {
            // must be float64 type
            DCHECK_EQ(_type, SchemaType::FLOAT64);
            return get_value<double>(row);
        }

        std::string_view get_text(const void *row) const override {
            // must be text type
            DCHECK_EQ(_type, SchemaType::TEXT);

            auto e_row = reinterpret_cast<const Extent::Row *>(row);
            uint32_t var_off;
            std::memcpy(&var_off, e_row->data() + _offset, sizeof(uint32_t));
            return e_row->get_text(var_off);
        }

        const std::shared_ptr<numeric::NumericData> get_numeric(const void *row) const override {
            // must be numeric type
            DCHECK_EQ(_type, SchemaType::NUMERIC);

            auto e_row = reinterpret_cast<const Extent::Row *>(row);
            uint32_t var_off;
            std::memcpy(&var_off, e_row->data() + _offset, sizeof(uint32_t));
            std::span<const char> numeric_data = e_row->get_binary(var_off);
            void *data_ptr = const_cast<char *>(numeric_data.data());
            auto ret = std::shared_ptr<numeric::NumericData>(
                reinterpret_cast<const numeric::Numeric>(data_ptr),
                [](numeric::Numeric) {});
            return ret;
        }

        const std::span<const char> get_binary(const void *row) const override {
            // must be binary type
            DCHECK_EQ(_type, SchemaType::BINARY);

            auto e_row = reinterpret_cast<const Extent::Row *>(row);
            uint32_t var_off;
            std::memcpy(&var_off, e_row->data() + _offset, sizeof(uint32_t));
            return e_row->get_binary(var_off);
        }

        void set_null(void *row, bool is_null) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            _set_bit(*e_row, _null_offset, _null_bitmask, is_null);
        }

        void set_bool(void *row, bool val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            _set_bit(*e_row, _offset, _bool_bitmask, val);
        }

        void set_int8(void *row, int8_t val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            *(e_row->data() + _offset) = *reinterpret_cast<char *>(&val);
        }

        void set_uint8(void *row, uint8_t val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            *(e_row->data() + _offset) = *reinterpret_cast<char *>(&val);
        }

        void set_int16(void *row, int16_t val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            std::memcpy(e_row->data() + _offset, &val, sizeof(int16_t));
        }

        void set_uint16(void *row, uint16_t val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            std::memcpy(e_row->data() + _offset, &val, sizeof(uint16_t));
        }

        void set_int32(void *row, int32_t val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            std::memcpy(e_row->data() + _offset, &val, sizeof(int32_t));
        }

        void set_uint32(void *row, uint32_t val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            std::memcpy(e_row->data() + _offset, &val, sizeof(uint32_t));
        }

        void set_int64(void *row, int64_t val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            std::memcpy(e_row->data() + _offset, &val, sizeof(int64_t));
        }

        void set_uint64(void *row, uint64_t val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            std::memcpy(e_row->data() + _offset, &val, sizeof(uint64_t));
        }

        void set_float32(void *row, float val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            std::memcpy(e_row->data() + _offset, &val, sizeof(float));
        }

        void set_float64(void *row, double val) override {
            auto e_row = reinterpret_cast<Extent::Row *>(row);
            std::memcpy(e_row->data() + _offset, &val, sizeof(double));
        }

        void set_text(void *row, const std::string_view &value) override {
            DCHECK_EQ(_type, SchemaType::TEXT);

            auto e_row = reinterpret_cast<Extent::Row *>(row);

            // store the string into the variable data
            uint32_t offset = e_row->set_text(value);

            // store the offset into the fixed data
            std::memcpy(e_row->data() + _offset, &offset, sizeof(uint32_t));
        }

        void set_numeric(void *row, const std::shared_ptr<numeric::NumericData> value) override {
            DCHECK_EQ(_type, SchemaType::NUMERIC);

            auto e_row = reinterpret_cast<Extent::Row *>(row);

            std::span<const char> numeric_data(reinterpret_cast<const char *>(value.get()), value->varsize());

            // store the numeric into the variable data
            uint32_t offset = e_row->set_binary(numeric_data);

            // store the offset into the fixed data
            std::memcpy(e_row->data() + _offset, &offset, sizeof(uint32_t));
        }

        void set_binary(void *row, const std::span<const char> &value) override {
            DCHECK_EQ(_type, SchemaType::BINARY);

            auto e_row = reinterpret_cast<Extent::Row *>(row);

            // store the string into the variable data
            uint32_t offset = e_row->set_binary(value);

            // store the offset into the fixed data
            std::memcpy(e_row->data() + _offset, &offset, sizeof(uint32_t));
        }


    private:
        bool
        _get_bit(const Extent::Row &row,
                 uint32_t offset,
                 uint8_t mask) const
        {
            return ((*reinterpret_cast<const uint8_t * const>(row.data() + offset) & mask) > 0);
        }

        void
        _set_bit(Extent::Row &row,
                 uint32_t offset,
                 uint8_t mask,
                 bool bit)
        {
            char * const fixed = row.data() + offset;

            char char_mask = static_cast<char>(-1) & mask;
            if (bit) {
                *fixed |= char_mask;
            } else {
                *fixed &= ~char_mask;
            }
        }

    private:
        SchemaType _type;

        bool _can_null;
        bool _can_undefined;

        uint32_t _offset;         ///< byte offset of the field in the row data
        uint8_t _bool_bitmask;

        uint32_t _null_offset;
        uint8_t _null_bitmask;

        uint32_t _undefined_offset;
        uint8_t _undefined_bitmask;
    };
    using ExtentFieldPtr = std::shared_ptr<ExtentField>;


    /**
     * A parent class for any constant fields.  Has no implementation, just used as a parent to all
     * other constant field types.
     */
    class ConstField : public Field {
        // intentionally empty
    };
    using ConstFieldPtr = std::shared_ptr<ConstField>;

    /**
     * A typed constant value that is represented as a Field for use in conditionals and joins.
     */
    template <class T>
    class ConstTypeField : public ConstField {
    private:
        T _value;

    public:
        explicit ConstTypeField(const T &value)
            : _value(value)
        { }

        explicit ConstTypeField(T &&value)
            : _value(std::move(value))
        { }

        SchemaType get_type() const override {
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
            } else if constexpr(std::is_same_v<T, std::shared_ptr<numeric::NumericData>>) {
                return SchemaType::NUMERIC;
            } else if constexpr(std::is_same_v<T, std::vector<char>>) {
                return SchemaType::BINARY;
            }

            throw TypeError();
        }

        bool get_bool(const void *row) const override {
            if constexpr(std::is_same_v<T, bool>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        int8_t get_int8(const void *row) const override {
            if constexpr(std::is_same_v<T, int8_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        uint8_t get_uint8(const void *row) const override {
            if constexpr(std::is_same_v<T, uint8_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        int16_t get_int16(const void *row) const override {
            if constexpr(std::is_same_v<T, int16_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        uint16_t get_uint16(const void *row) const override {
            if constexpr(std::is_same_v<T, uint16_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        int32_t get_int32(const void *row) const override {
            if constexpr(std::is_same_v<T, int32_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        uint32_t get_uint32(const void *row) const override {
            if constexpr(std::is_same_v<T, uint32_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        int64_t get_int64(const void *row) const override {
            if constexpr(std::is_same_v<T, int64_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        uint64_t get_uint64(const void *row) const override {
            if constexpr(std::is_same_v<T, uint64_t>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        float get_float32(const void *row) const override {
            if constexpr(std::is_same_v<T, float>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        double get_float64(const void *row) const override {
            if constexpr(std::is_same_v<T, double>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        std::string_view get_text(const void *row) const override {
            if constexpr(std::is_same_v<T, std::string>) {
                return _value;
            } else {
                throw TypeError();
            }
        }

        const std::shared_ptr<numeric::NumericData> get_numeric(const void *row) const override {
            if constexpr(std::is_same_v<T, std::shared_ptr<numeric::NumericData>>) {
                // LOG_INFO("---> ConstField: value = {}", _value->to_string());
                return _value;
            } else {
                throw TypeError();
            }
        }

        const std::span<const char> get_binary(const void *row) const override {
            if constexpr(std::is_same_v<T, std::vector<char>>) {
                return _value;
            } else {
                throw TypeError();
            }
        }
    };
    template <class T>
    using ConstTypeFieldPtr = std::shared_ptr<ConstTypeField<T>>;

    /**
     * A value that is always null represented as a Field for use in conditionals and joins.
     */
    class ConstNullField : public ConstField {
    private:
        SchemaType _type;

    public:
        explicit ConstNullField(SchemaType type)
            : _type(type)
        { }

        virtual SchemaType get_type() const override {
            return _type;
        }

        bool can_be_null() const override {
            return true;
        }

        bool is_null(const void *row) const override {
            return true;
        }
    };

    /**
     * A field that wraps another field to return a default value if the underlying value is null.
     */
    template <class T>
    class DefaultValueField : public Field {
    private:
        FieldPtr _field;
        T _default;

    public:
        DefaultValueField(FieldPtr field, const T &default_value)
            : _field(field), _default(default_value)
        { }

        DefaultValueField(FieldPtr field, T &&default_value)
            : _field(field), _default(default_value)
        { }

        virtual SchemaType get_type() const override {
            return _field->get_type();
        }

        bool can_be_null() const override {
            return false;
        }

        bool is_null(const void *row) const override {
            return false;
        }

        bool get_bool(const void *row) const override {
            if constexpr(std::is_same_v<T, bool>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_bool(row);
            } else {
                throw TypeError();
            }
        }

        int8_t get_int8(const void *row) const override {
            if constexpr(std::is_same_v<T, int8_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_int8(row);
            } else {
                throw TypeError();
            }
        }

        uint8_t get_uint8(const void *row) const override {
            if constexpr(std::is_same_v<T, uint8_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_uint8(row);
            } else {
                throw TypeError();
            }
        }

        int16_t get_int16(const void *row) const override {
            if constexpr(std::is_same_v<T, int16_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_int16(row);
            } else {
                throw TypeError();
            }
        }

        uint16_t get_uint16(const void *row) const override {
            if constexpr(std::is_same_v<T, uint16_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_uint16(row);
            } else {
                throw TypeError();
            }
        }

        int32_t get_int32(const void *row) const override {
            if constexpr(std::is_same_v<T, int32_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_int32(row);
            } else {
                throw TypeError();
            }
        }

        uint32_t get_uint32(const void *row) const override {
            if constexpr(std::is_same_v<T, uint32_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_uint32(row);
            } else {
                throw TypeError();
            }
        }

        int64_t get_int64(const void *row) const override {
            if constexpr(std::is_same_v<T, int64_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_int64(row);
            } else {
                throw TypeError();
            }
        }

        uint64_t get_uint64(const void *row) const override {
            if constexpr(std::is_same_v<T, uint64_t>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_uint64(row);
            } else {
                throw TypeError();
            }
        }

        float get_float32(const void *row) const override {
            if constexpr(std::is_same_v<T, float>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_float32(row);
            } else {
                throw TypeError();
            }
        }

        double get_float64(const void *row) const override {
            if constexpr(std::is_same_v<T, double>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_float64(row);
            } else {
                throw TypeError();
            }
        }

        std::string_view get_text(const void *row) const override {
            if constexpr(std::is_same_v<T, std::string>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_text(row);
            } else {
                throw TypeError();
            }
        }

        const std::shared_ptr<numeric::NumericData> get_numeric(const void *row) const override {
            if constexpr(std::is_same_v<T, std::shared_ptr<numeric::NumericData>>) {
                if (_field->is_null(row)) {
                    LOG_INFO("---> DefaultValueField: value = {}", _default->to_string());
                    return _default;
                }
                return _field->get_numeric(row);
            } else {
                throw TypeError();
            }
        }

        const std::span<const char> get_binary(const void *row) const override {
            if constexpr(std::is_same_v<T, std::vector<char>>) {
                if (_field->is_null(row)) {
                    return _default;
                }
                return _field->get_binary(row);
            } else {
                throw TypeError();
            }
        }
    };

    // FIELD ARRAYS

    using FieldArray = std::vector<FieldPtr>;
    using FieldArrayPtr = std::shared_ptr<FieldArray>;

    using MutableFieldArray = std::vector<MutableFieldPtr>;
    using MutableFieldArrayPtr = std::shared_ptr<MutableFieldArray>;

    /**
     * An array of values, encapsulated such that they can be compared even when coming from
     * different tables or different rows within a table, or are just fixed values (e.g., ValueTuple).
     */
    class Tuple {
    public:
        explicit Tuple(const void *row)
            : _row(row)
        { }
        virtual ~Tuple() = default;

        const void *row() const
        {
            return _row;
        }

        virtual std::size_t size() const = 0;
        virtual FieldPtr field(int idx) const = 0;
        virtual FieldArrayPtr fields() const = 0;

        bool less_than(std::shared_ptr<Tuple> rhs, bool nulls_last=true) const {
            return this->less_than(*rhs, nulls_last);
        }

        bool less_than(const Tuple &rhs, bool nulls_last=true) const {
            // use the minimum size of the two tuples for comparison
            int size = std::min(this->size(), rhs.size());

            for (int i = 0; i < size; i++) {
                DCHECK_EQ(this->field(i)->get_type(), rhs.field(i)->get_type());
            }

            // XXX switch everything to compare()?
            for (int i = 0; i < size; i++) {
                if (this->field(i)->less_than(this->row(), rhs.field(i), rhs.row(), nulls_last)) {
                    return true;
                }
                if (rhs.field(i)->less_than(rhs.row(), this->field(i), this->row(), nulls_last)) {
                    return false;
                }
            }

            // all values are equal, so not less than
            return false;
        }

        bool equal_strict(const Tuple &rhs) const {
            // check the tuple lengths and types using CHECK()
            // we assume correct usage in production
            CHECK_EQ(this->size(), rhs.size());
            return _equal(rhs, size());
        }

        bool equal_prefix(const Tuple &rhs) const {
            size_t size = std::min(this->size(), rhs.size());
            return _equal(rhs, size);
        }

        std::string to_string() const {
            std::string value;
            for (int i = 0; i < this->size(); i++) {
                if (this->field(i)->is_null(this->row())) {
                    value += "NULL:";
                    continue;
                }

                switch (this->field(i)->get_type()) {
                case SchemaType::BOOLEAN:
                    value += fmt::format("{}:", this->field(i)->get_bool(this->row()));
                    break;

                case SchemaType::UINT8:
                    value += fmt::format("{}:", this->field(i)->get_uint8(this->row()));
                    break;

                case SchemaType::INT8:
                    value += fmt::format("{}:", this->field(i)->get_int8(this->row()));
                    break;

                case SchemaType::UINT16:
                    value += fmt::format("{}:", this->field(i)->get_uint16(this->row()));
                    break;

                case SchemaType::INT16:
                    value += fmt::format("{}:", this->field(i)->get_int16(this->row()));
                    break;

                case SchemaType::UINT32:
                    value += fmt::format("{}:", this->field(i)->get_uint32(this->row()));
                    break;

                case SchemaType::INT32:
                    value += fmt::format("{}:", this->field(i)->get_int32(this->row()));
                    break;

                case SchemaType::UINT64:
                    value += fmt::format("{}:", this->field(i)->get_uint64(this->row()));
                    break;

                case SchemaType::INT64:
                    value += fmt::format("{}:", this->field(i)->get_int64(this->row()));
                    break;

                case SchemaType::FLOAT32:
                    value += fmt::format("{}:", this->field(i)->get_float32(this->row()));
                    break;

                case SchemaType::FLOAT64:
                    value += fmt::format("{}:", this->field(i)->get_float64(this->row()));
                    break;

                case SchemaType::TEXT:
                    value += fmt::format("{}:", this->field(i)->get_text(this->row()));
                    break;

                case SchemaType::NUMERIC:
                    value += fmt::format("{}:", this->field(i)->get_numeric(this->row())->to_debug_string());
                    break;

                case SchemaType::BINARY:
                    value += fmt::format("<binary>:");
                    break;

                default:
                    LOG_ERROR("Unsupported data type: {}", (int)this->field(i)->get_type());
                    throw TypeError();
                }
            }
            return value.substr(0, value.size() - 1);
        }

    private:
        const void *_row;

        bool _equal(const Tuple &rhs, size_t size) const {
            CHECK(size);
            for (int i = 0; i != size; ++i) {
                CHECK_EQ(this->field(i)->get_type(), rhs.field(i)->get_type());
                if (!this->field(i)->equal(this->row(), rhs.field(i), rhs.row())) {
                    return false;
                }
            }

            // all values are equal
            return true;
        }
    };
    using TuplePtr = std::shared_ptr<Tuple>;

    class FieldTuple : public Tuple {
    public:
        FieldTuple(const FieldTuple &tuple) = default;
        FieldTuple(FieldTuple &&tuple) noexcept = default;
        FieldTuple(FieldArrayPtr array, const void *row)
            : Tuple(row),
              _array(array)
        { }

        std::size_t size() const override {
            return _array->size();
        }

        FieldPtr field(int idx) const override {
            return _array->at(idx);
        }

        FieldArrayPtr fields() const override {
            return _array;
        }

    private:
        FieldArrayPtr _array;
    };
    using FieldTuplePtr = std::shared_ptr<FieldTuple>;


    class KeyValueTuple : public Tuple {
    public:
        KeyValueTuple(FieldArrayPtr key, FieldArrayPtr value, const void *row)
            : Tuple(row),
              _key(key),
              _value(value)
        { }

        std::size_t size() const override {
            return _key->size() + _value->size();
        }

        FieldPtr field(int idx) const override {
            if (idx < _key->size()) {
                return _key->at(idx);
            } else {
                return _value->at(idx - _key->size());
            }
        }

        FieldArrayPtr fields() const override {
            auto combined = std::make_shared<FieldArray>();
            for (auto f : *_key) {
                combined->push_back(f);
            }
            for (auto f : *_value) {
                combined->push_back(f);
            }
            return combined;
        }

    private:
        FieldArrayPtr _key;
        FieldArrayPtr _value;
    };

    /**
     * Interface for a mutable Tuple that can have it's value set by another Tuple.
     */
    class MutableTuple : public Tuple {
    public:
        MutableTuple(MutableFieldArrayPtr array,
                     void *row)
            : Tuple(row),
              _array(array)
        { DCHECK_NE(_array, nullptr); }

        void *mutable_row() const {
            return const_cast<void *>(this->row());
        }

        std::size_t size() const override {
            return _array->size();
        }

        FieldPtr field(int idx) const override {
            return _array->at(idx);
        }

        FieldArrayPtr fields() const override {
            auto array = std::make_shared<FieldArray>();
            for (auto f : *_array) {
                array->push_back(f);
            }
            return array;
        }

        MutableFieldPtr mutable_field(int idx) const {
            return _array->at(idx);
        }

        /** Copy the data from another Tuple into this Tuple. */
        void assign(TuplePtr other)
        {
            for (int i = 0; i < size(); i++) {
                mutable_field(i)->set_field(this->mutable_row(), other->field(i), other->row());
            }
        }

        void assign(const Tuple &other)
        {
            for (int i = 0; i < size(); i++) {
                mutable_field(i)->set_field(this->mutable_row(), other.field(i), other.row());
            }
        }

    private:
        MutableFieldArrayPtr _array;
    };
    using MutableTuplePtr = std::shared_ptr<MutableTuple>;


    class PgLogField : public Field {
    public:
        PgLogField(SchemaType type, uint32_t offset)
            : _type(type),
              _offset(offset)
        { }

        SchemaType get_type() const override {
            return _type;
        }

        bool can_be_undefined() const override {
            return true;
        }

        bool can_be_null() const override {
            return true;
        }

        bool is_undefined(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            if (data->tuple_data[_offset].type == 'u') {
                return true;
            }
            return false;
        }

        bool is_null(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            return data->tuple_data[_offset].type == 'n';
        }

        bool get_bool(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            const PgMsgTupleDataColumn &col = data->tuple_data[_offset];

            // XXX we only support binary data for native types
            DCHECK_EQ(col.type, 'b');

            // boolean should 1 byte
            CHECK_EQ(col.data.size(), 1);

            // read in the binary data and convert to a boolean
            return (col.data[0] == 1);
        }

        int16_t get_int16(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            const PgMsgTupleDataColumn &col = data->tuple_data[_offset];

            // XXX we only support binary data for native types
            DCHECK_EQ(col.type, 'b');

            // int16 should be 2 bytes
            CHECK_EQ(col.data.size(), 2);

            // read in the binary data and convert to a 16-bit int
            return recvint16(col.data.data());
        }

        int32_t get_int32(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            const PgMsgTupleDataColumn &col = data->tuple_data[_offset];

            // XXX we only support binary data for native types
            DCHECK_EQ(col.type, 'b');

            // int32 should be 4 bytes
            CHECK_EQ(col.data.size(), 4);

            // read in the binary data and convert to a 32-bit int
            return recvint32(col.data.data());
        }

        int64_t get_int64(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            const PgMsgTupleDataColumn &col = data->tuple_data[_offset];

            // XXX we only support binary data for native types
            DCHECK_EQ(col.type, 'b');

            // int64 should be 8 bytes
            CHECK_EQ(col.data.size(), 8);

            // read in the binary data and convert to a 64-bit int
            return recvint64(col.data.data());
        }

        float get_float32(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            const PgMsgTupleDataColumn &col = data->tuple_data[_offset];

            // XXX we only support binary data for native types
            DCHECK_EQ(col.type, 'b');

            // float should be 4 bytes
            CHECK_EQ(col.data.size(), 4);

            // read in the binary data and convert to a 32-bit float
            int32_t value = recvint32(col.data.data());
            return std::bit_cast<float>(value);
        }

        double get_float64(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            const PgMsgTupleDataColumn &col = data->tuple_data[_offset];

            // XXX we only support binary data for native types
            DCHECK_EQ(col.type, 'b');

            // double should be 8 bytes
            CHECK_EQ(col.data.size(), 8);

            // read in the binary data and convert to a 64-bit float
            int64_t value = recvint64(col.data.data());
            return std::bit_cast<double>(value);
        }

        std::string_view get_text(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            const PgMsgTupleDataColumn &col = data->tuple_data[_offset];

            // XXX we only support binary data for native types
            DCHECK_EQ(col.type, 'b');

            // read in the binary data as a string
            return std::string_view(col.data.data(), col.data.size());
        }

        /*
        std::string binary_to_hex(const char* data, size_t length) const {
            std::stringstream ss;
            ss << std::hex << std::setfill('0');
            for (size_t i = 0; i < length; ++i) {
                ss << std::setw(2) << static_cast<unsigned int>(static_cast<unsigned char>(data[i]));
            }
            return ss.str();
        }
        */

        const std::shared_ptr<numeric::NumericData> get_numeric(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            const PgMsgTupleDataColumn &col = data->tuple_data[_offset];

            // XXX we only support binary data for native types
            DCHECK_EQ(col.type, 'b');

            numeric::Numeric value = numeric::numeric_receive(col.data.begin().base(), col.data.size(), 0);
            return std::shared_ptr<numeric::NumericData>(value, [](numeric::Numeric ptr) {
                numeric::NumericData::free_numeric(ptr);
            });
        }

        const std::span<const char> get_binary(const void *row) const override {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);
            const PgMsgTupleDataColumn &col = data->tuple_data[_offset];

            // XXX we only support binary data for native types
            DCHECK_EQ(col.type, 'b');

            // read in the binary data as a string
            return std::span<const char>(col.data.begin(), col.data.size());
        }

    protected:
        SchemaType _type;
        uint32_t _offset;  ///< Column offset in the tuple data
    };
    using PgLogFieldPtr = std::shared_ptr<PgLogField>;

    /**
     * @brief Field class to wrap a Postgres enum user defined type
     */
    class PgEnumField : public PgLogField {
    public:
        PgEnumField(SchemaType type, uint32_t offset, UserTypePtr ut)
            : PgLogField(type, offset), _ut(ut)
        {
            DCHECK_EQ(type, SchemaType::FLOAT32);
            DCHECK_NE(ut, nullptr);
            DCHECK_EQ(ut->type, UserType::ENUM);
        }

        bool is_null(const void *row) const override
        {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);

            // check if the enum is null
            return (!_ut->exists || data->tuple_data[_offset].type == 'n');
        }

        float get_float32(const void *row) const override
        {
            auto &&data = reinterpret_cast<PgMsgTupleData const *>(row);

            // get the label from the tuple data
            std::string label = std::string(data->tuple_data[_offset].data.begin(),
            data->tuple_data[_offset].data.end());

            // lookup the label in the enum map
            float index = _ut->enum_label_map.at(label);
            return index;
        }

    private:
        UserTypePtr _ut;
    };
    using PgEnumFieldPtr = std::shared_ptr<PgEnumField>;

    class ValueTuple : public Tuple {
        /** An array of ConstField objects. */
        std::vector<ConstFieldPtr> _fields;

    public:
        ValueTuple()
            : Tuple(nullptr)
        { }

        explicit ValueTuple(TuplePtr tuple)
            : Tuple(nullptr)
        {
            // copy the data from the tuple into const fields
            for (auto i = 0; i < tuple->size(); i++) {
                std::shared_ptr<Field> field = tuple->field(i);

                if (field->is_null(tuple->row())) {
                    _fields.push_back(std::make_shared<ConstNullField>(field->get_type()));
                } else {
                    switch(tuple->field(i)->get_type()) {
                    case(SchemaType::TEXT):
                        {
                            // note: need to perform a copy here to store the constant string
                            std::string value(field->get_text(tuple->row()));
                            _fields.push_back(std::make_shared<ConstTypeField<std::string>>(std::move(value)));
                        }
                        break;
                    case(SchemaType::UINT64):
                        _fields.push_back(std::make_shared<ConstTypeField<uint64_t>>(field->get_uint64(tuple->row())));
                        break;
                    case(SchemaType::INT64):
                        _fields.push_back(std::make_shared<ConstTypeField<int64_t>>(field->get_int64(tuple->row())));
                        break;
                    case(SchemaType::UINT32):
                        _fields.push_back(std::make_shared<ConstTypeField<uint32_t>>(field->get_uint32(tuple->row())));
                        break;
                    case(SchemaType::INT32):
                        _fields.push_back(std::make_shared<ConstTypeField<int32_t>>(field->get_int32(tuple->row())));
                        break;
                    case(SchemaType::UINT16):
                        _fields.push_back(std::make_shared<ConstTypeField<uint16_t>>(field->get_uint16(tuple->row())));
                        break;
                    case(SchemaType::INT16):
                        _fields.push_back(std::make_shared<ConstTypeField<int16_t>>(field->get_int16(tuple->row())));
                        break;
                    case(SchemaType::UINT8):
                        _fields.push_back(std::make_shared<ConstTypeField<uint8_t>>(field->get_uint8(tuple->row())));
                        break;
                    case(SchemaType::INT8):
                        _fields.push_back(std::make_shared<ConstTypeField<int8_t>>(field->get_int8(tuple->row())));
                        break;
                    case(SchemaType::BOOLEAN):
                        _fields.push_back(std::make_shared<ConstTypeField<bool>>(field->get_bool(tuple->row())));
                        break;
                    case(SchemaType::FLOAT64):
                        _fields.push_back(std::make_shared<ConstTypeField<double>>(field->get_float64(tuple->row())));
                        break;
                    case(SchemaType::FLOAT32):
                        _fields.push_back(std::make_shared<ConstTypeField<float>>(field->get_float32(tuple->row())));
                        break;
                    case(SchemaType::NUMERIC):
                        {
                            // note: perform a copy here to store the constant value
                            auto &&numeric_data = field->get_numeric(tuple->row());
                            _fields.push_back(std::make_shared<ConstTypeField<std::shared_ptr<numeric::NumericData>>>(
                                std::move(numeric_data)
                            ));
                        }
                        break;
                    case(SchemaType::BINARY):
                        {
                            // note: perform a copy here to store the constant value
                            auto &&tmp = field->get_binary(tuple->row());
                            std::vector<char> value(tmp.begin(), tmp.end());
                            _fields.push_back(std::make_shared<ConstTypeField<std::vector<char>>>(std::move(value)));
                        }
                        break;
                    default:
                        throw TypeError();
                    }
                }
            }
        }

        explicit ValueTuple(const std::vector<ConstFieldPtr> &fields)
            : Tuple(nullptr),
              _fields(fields)
        { }

        std::size_t size() const override {
            return _fields.size();
        }

        FieldPtr field(int idx) const override {
            return _fields[idx];
        }

        FieldArrayPtr fields() const override {
            auto array = std::make_shared<FieldArray>();
            for (auto f : _fields) {
                array->push_back(f);
            }
            return array;
        }
    };
    typedef std::shared_ptr<ValueTuple> ValueTuplePtr;

}
