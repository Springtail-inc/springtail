#include <vincentlaucsb-csv-parser/csv.hpp>

#include <storage/field.hh>

namespace springtail {
    
    class CSVField : public Field {
    private:
        mutable csv::CSVField _field;
        SchemaType _type;

    public:
        CSVField(const csv::CSVField &field, SchemaType type)
            : _field(field), _type(type)
        { }

        SchemaType get_type() const override {
            return _type;
        }

        bool get_bool(const Extent::Row &row) const override {
            std::string s = _field.get<std::string>();
            return (s.compare("true") == 0 ||
                    s.compare("TRUE") == 0 ||
                    s.compare("t") == 0 ||
                    s.compare("T") == 0);
        }

        virtual int8_t get_int8(const Extent::Row &row) const override {
            return _field.get<int8_t>();
        }

        virtual uint8_t get_uint8(const Extent::Row &row) const override {
            return _field.get<uint8_t>();
        }

        virtual int16_t get_int16(const Extent::Row &row) const override {
            return _field.get<int16_t>();
        }

        virtual uint16_t get_uint16(const Extent::Row &row) const override {
            return _field.get<uint16_t>();
        }

        virtual int32_t get_int32(const Extent::Row &row) const override {
            return _field.get<int32_t>();
        }

        virtual uint32_t get_uint32(const Extent::Row &row) const override {
            return _field.get<uint32_t>();
        }

        virtual int64_t get_int64(const Extent::Row &row) const override {
            return _field.get<int64_t>();
        }

        virtual uint64_t get_uint64(const Extent::Row &row) const override {
            return _field.get<uint64_t>();
        }

        virtual float get_float32(const Extent::Row &row) const override {
            return _field.get<float>();
        }

        virtual double get_float64(const Extent::Row &row) const override {
            return _field.get<double>();
        }

        virtual std::string get_text(const Extent::Row &row) const override {
            return _field.get();
        }

        bool less_than(const Extent::Row &lhs_row,
                       std::shared_ptr<Field> rhs,
                       const Extent::Row &rhs_row,
                       bool nulls_last=true) const override
        {
            // if the rhs is null, then it is always either larger or smaller than a non-null value
            if (rhs->is_null(rhs_row)) {
                return nulls_last;
            }

            // compare the values
            switch (_type) {
            case(SchemaType::TEXT):
                return (this->get_text(lhs_row) < rhs->get_text(rhs_row));
            case(SchemaType::UINT64):
                return (this->get_uint64(lhs_row) < rhs->get_uint64(rhs_row));
            case(SchemaType::INT64):
                return (this->get_int64(lhs_row) < rhs->get_int64(rhs_row));
            case(SchemaType::UINT32):
                return (this->get_uint32(lhs_row) < rhs->get_uint32(rhs_row));
            case(SchemaType::INT32):
                return (this->get_int32(lhs_row) < rhs->get_int32(rhs_row));
            case(SchemaType::UINT16):
                return (this->get_uint16(lhs_row) < rhs->get_uint16(rhs_row));
            case(SchemaType::INT16):
                return (this->get_int16(lhs_row) < rhs->get_int16(rhs_row));
            case(SchemaType::UINT8):
                return (this->get_uint8(lhs_row) < rhs->get_uint8(rhs_row));
            case(SchemaType::INT8):
                return (this->get_int8(lhs_row) < rhs->get_int8(rhs_row));
            case(SchemaType::BOOLEAN):
                return (this->get_bool(lhs_row) < rhs->get_bool(rhs_row));
            case(SchemaType::FLOAT64):
                return (this->get_float64(lhs_row) < rhs->get_float64(rhs_row));
            case(SchemaType::FLOAT32):
                return (this->get_float32(lhs_row) < rhs->get_float32(rhs_row));
            case(SchemaType::BINARY):
            default:
                throw TypeError();
            }
        }
    };
    typedef std::shared_ptr<CSVField> CSVFieldPtr;

    class CSVTuple : public Tuple {
    private:
        csv::CSVRow _row;
        FieldArrayPtr _fields;

    public:
        CSVTuple(csv::CSVRow row, FieldArrayPtr fields)
            : _row(row), _fields(fields)
        { }

        std::size_t
        size() const override
        {
            return _row.size();
        }

        std::shared_ptr<Field>
        field(int idx) const override
        {
            return std::make_shared<CSVField>(_row[idx], (*_fields)[idx]->get_type());
        }

        Extent::Row
        row() const override
        {
            return Extent::Row(nullptr, 0);
        }
    };
}
