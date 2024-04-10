#pragma once

#include <storage/field.hh>

namespace springtail::sys_tbl {

    /**
     * Helper functions and constants for the table_names table.
     */
    class TableNames {
    public:
        static constexpr uint32_t ID = 1;

        struct Data {
            static constexpr uint32_t NAMESPACE = 0;
            static constexpr uint32_t NAME = 1;
            static constexpr uint32_t TABLE_ID = 2;
            static constexpr uint32_t XID = 3;
            static constexpr uint32_t LSN = 4;
            static constexpr uint32_t EXISTS = 5;

            static const std::vector<SchemaColumn> SCHEMA;

            static TuplePtr
            tuple(const std::string &schema,
                  const std::string &name,
                  uint64_t table_id,
                  uint64_t xid,
                  uint64_t lsn,
                  bool exists)
            {
                auto fields = std::make_shared<FieldArray>(6);
                fields->at(NAMESPACE) = std::make_shared<ConstTypeField<std::string>>(schema);
                fields->at(NAME) = std::make_shared<ConstTypeField<std::string>>(name);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(LSN) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
                fields->at(EXISTS) = std::make_shared<ConstTypeField<bool>>(exists);
                return std::make_shared<FieldTuple>(fields, nullptr);
            }
        };

        struct Primary {
            static constexpr uint32_t NAMESPACE = 0;
            static constexpr uint32_t NAME = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t LSN = 3;
            static constexpr uint32_t EXTENT_ID = 4;

            static const std::vector<SchemaColumn> SCHEMA;
            static const std::vector<std::string> KEY;
        };

        struct Secondary {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t XID = 1;
            static constexpr uint32_t LSN = 2;
            static constexpr uint32_t EXTENT_ID = 3;
            static constexpr uint32_t ROW_ID = 4;

            static const std::vector<SchemaColumn> SCHEMA;
            static const std::vector<std::string> KEY;

            static TuplePtr
            key_tuple(uint64_t table_id,
                      uint64_t xid,
                      uint64_t lsn)
            {
                auto fields = std::make_shared<FieldArray>(3);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(LSN) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
                return std::make_shared<FieldTuple>(fields, nullptr);
            }
        };
    };

    /**
     * Helper functions and constants for the table_roots table.
     */
    class TableRoots {
    public:
        static constexpr uint32_t ID = 2;

        struct Data {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t INDEX_ID = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t EXTENT_ID = 3;

            static const std::vector<SchemaColumn> SCHEMA;

            static TuplePtr
            tuple(uint64_t table_id,
                  uint64_t index_id,
                  uint64_t xid)
            {
                auto fields = std::make_shared<FieldArray>(4);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(EXTENT_ID) = std::make_shared<ConstNullField>(SchemaType::UINT64);
                return std::make_shared<FieldTuple>(fields, nullptr);
            }

            static TuplePtr
            tuple(uint64_t table_id,
                  uint64_t index_id,
                  uint64_t xid,
                  uint64_t extent_id)
            {
                auto fields = std::make_shared<FieldArray>(4);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(EXTENT_ID) = std::make_shared<ConstTypeField<uint64_t>>(extent_id);
                return std::make_shared<FieldTuple>(fields, nullptr);
            }
        };

        struct Primary {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t INDEX_ID = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t EXTENT_ID = 3;

            static const std::vector<SchemaColumn> SCHEMA;
            static const std::vector<std::string> KEY;

            static TuplePtr
            key_tuple(uint64_t table_id, uint64_t index_id, uint64_t xid) {
                auto key_fields = std::make_shared<FieldArray>(3);
                key_fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                key_fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                key_fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                return std::make_shared<FieldTuple>(key_fields, nullptr);
            }
        };
    };

    /**
     * Helper functions and constants for the table_names table.
     */
    class Indexes {
    public:
        static constexpr uint32_t ID = 3;

        struct Data {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t INDEX_ID = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t POSITION = 3;
            static constexpr uint32_t COLUMN_ID = 4;

            static const std::vector<SchemaColumn> SCHEMA;

            static FieldArrayPtr
            fields(uint64_t table_id,
                   uint64_t index_id,
                   uint64_t xid,
                   uint32_t position,
                   uint32_t column_id)
            {
                auto fields = std::make_shared<FieldArray>(4);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(POSITION) = std::make_shared<ConstTypeField<uint32_t>>(position);
                fields->at(COLUMN_ID) = std::make_shared<ConstTypeField<uint32_t>>(column_id);
                return fields;
            }
        };

        struct Primary {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t INDEX_ID = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t POSITION = 3;
            static constexpr uint32_t EXTENT_ID = 4;

            static const std::vector<SchemaColumn> SCHEMA;
            static const std::vector<std::string> KEY;

            static TuplePtr
            key_tuple(uint64_t table_id, uint64_t index_id, uint64_t xid, uint32_t position) {
                auto key_fields = std::make_shared<FieldArray>(4);
                key_fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                key_fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                key_fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                key_fields->at(POSITION) = std::make_shared<ConstTypeField<uint32_t>>(position);
                return std::make_shared<FieldTuple>(key_fields, nullptr);
            }
        };
    };

    class Schemas {
    public:
        static constexpr uint32_t ID = 4;

        struct Data {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t POSITION = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t LSN = 3;
            static constexpr uint32_t EXISTS = 4;
            static constexpr uint32_t NAME = 5;
            static constexpr uint32_t TYPE = 6;
            static constexpr uint32_t NULLABLE = 7;
            static constexpr uint32_t DEFAULT = 8;
            static constexpr uint32_t UPDATE_TYPE = 9;

            static const std::vector<SchemaColumn> SCHEMA;

            static TuplePtr
            tuple(uint64_t table_id,
                  uint32_t position,
                  uint64_t xid,
                  uint64_t lsn,
                  bool exists,
                  const std::string &name,
                  uint8_t type,
                  bool nullable,
                  const std::optional<std::string> &default_value,
                  uint8_t update_type)
            {
                auto fields = std::make_shared<FieldArray>(10);

                fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(1) = std::make_shared<ConstTypeField<uint32_t>>(position);
                fields->at(2) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(3) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
                fields->at(4) = std::make_shared<ConstTypeField<bool>>(exists);
                fields->at(5) = std::make_shared<ConstTypeField<std::string>>(name);
                fields->at(6) = std::make_shared<ConstTypeField<uint8_t>>(type);
                fields->at(7) = std::make_shared<ConstTypeField<bool>>(nullable);
                if (default_value) {
                    fields->at(8) = std::make_shared<ConstTypeField<std::string>>(*default_value);
                } else {
                    fields->at(8) = std::make_shared<ConstNullField>(SchemaType::TEXT);
                }
                fields->at(9) = std::make_shared<ConstTypeField<uint8_t>>(update_type);

                return std::make_shared<FieldTuple>(fields, nullptr);
            }
        };

        struct Primary {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t POSITION = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t LSN = 3;
            static constexpr uint32_t EXTENT_ID = 4;

            static const std::vector<SchemaColumn> SCHEMA;
            static const std::vector<std::string> KEY;

            static TuplePtr
            key_tuple(uint64_t table_id, uint32_t position, uint64_t xid, uint64_t lsn) {
                auto key_fields = std::make_shared<FieldArray>(4);
                key_fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                key_fields->at(POSITION) = std::make_shared<ConstTypeField<uint32_t>>(position);
                key_fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                key_fields->at(LSN) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
                return std::make_shared<FieldTuple>(key_fields, nullptr);
            }
        };
    };

}
