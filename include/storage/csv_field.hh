#include <vincentlaucsb-csv-parser/csv.hpp>

#include <storage/field.hh>

namespace springtail {
    
    class CSVField : public Field {
    public:
        CSVField(SchemaType type, uint32_t idx)
            : _type(type), _idx(idx)
        { }

        SchemaType get_type() const override {
            return _type;
        }

        bool get_bool(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);

            std::string s = csv_row[_idx].get<std::string>();
            return (s.compare("true") == 0 ||
                    s.compare("TRUE") == 0 ||
                    s.compare("t") == 0 ||
                    s.compare("T") == 0);
        }

        int8_t get_int8(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<int8_t>();
        }

        uint8_t get_uint8(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<uint8_t>();
        }

        int16_t get_int16(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<int16_t>();
        }

        uint16_t get_uint16(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<uint16_t>();
        }

        int32_t get_int32(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<int32_t>();
        }

        uint32_t get_uint32(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<uint32_t>();
        }

        int64_t get_int64(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<int64_t>();
        }

        uint64_t get_uint64(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<uint64_t>();
        }

        float get_float32(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<float>();
        }

        double get_float64(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get<double>();
        }

        std::string_view get_text(const std::any &row) const override {
            auto &&csv_row = std::any_cast<csv::CSVRow>(row);
            return csv_row[_idx].get_sv();
        }

    private:
        SchemaType _type;
        uint32_t _idx;
    };
    typedef std::shared_ptr<CSVField> CSVFieldPtr;
}
