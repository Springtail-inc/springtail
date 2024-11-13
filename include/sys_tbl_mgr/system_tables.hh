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
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t XID = 1;
            static constexpr uint32_t LSN = 2;
            static constexpr uint32_t EXTENT_ID = 3;

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

        struct Secondary {
            static constexpr uint32_t NAMESPACE = 0;
            static constexpr uint32_t NAME = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t LSN = 3;
            static constexpr uint32_t EXTENT_ID = 4;
            static constexpr uint32_t ROW_ID = 5;

            static const std::vector<SchemaColumn> SCHEMA;
            static const std::vector<std::string> KEY;
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
            static constexpr uint32_t SNAPSHOT_XID = 4;

            static const std::vector<SchemaColumn> SCHEMA;

            static TuplePtr
            tuple(uint64_t table_id,
                  uint64_t index_id,
                  uint64_t xid,
                  uint64_t extent_id,
                  uint64_t snapshot_xid)
            {
                auto fields = std::make_shared<FieldArray>(5);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(EXTENT_ID) = std::make_shared<ConstTypeField<uint64_t>>(extent_id);
                fields->at(SNAPSHOT_XID) = std::make_shared<ConstTypeField<uint64_t>>(snapshot_xid);
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
            static constexpr uint32_t LSN = 3;
            static constexpr uint32_t POSITION = 4;
            static constexpr uint32_t COLUMN_ID = 5;

            static const std::vector<SchemaColumn> SCHEMA;

            static FieldArrayPtr
            fields(uint64_t table_id,
                   uint64_t index_id,
                   uint64_t xid,
                   uint64_t lsn,
                   uint32_t position,
                   uint32_t column_id
                   )
            {
                auto fields = std::make_shared<FieldArray>(6);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(LSN) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
                fields->at(POSITION) = std::make_shared<ConstTypeField<uint32_t>>(position);
                fields->at(COLUMN_ID) = std::make_shared<ConstTypeField<uint32_t>>(column_id);
                return fields;
            }
        };

        struct Primary {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t INDEX_ID = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t LSN = 3;
            static constexpr uint32_t POSITION = 4;
            static constexpr uint32_t EXTENT_ID = 5;

            static const std::vector<SchemaColumn> SCHEMA;
            static const std::vector<std::string> KEY;

            static TuplePtr
            key_tuple(uint64_t table_id, uint64_t index_id, uint64_t xid, uint64_t lsn, uint32_t position) {
                auto key_fields = std::make_shared<FieldArray>(5);
                key_fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                key_fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                key_fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                key_fields->at(LSN) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
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
            static constexpr uint32_t PG_TYPE = 7;
            static constexpr uint32_t NULLABLE = 8;
            static constexpr uint32_t DEFAULT = 9;
            static constexpr uint32_t UPDATE_TYPE = 10;

            static const std::vector<SchemaColumn> SCHEMA;

            static TuplePtr
            tuple(uint64_t table_id,
                  uint32_t position,
                  uint64_t xid,
                  uint64_t lsn,
                  bool exists,
                  const std::string &name,
                  uint8_t type,
                  int32_t pg_type,
                  bool nullable,
                  const std::optional<std::string> &default_value,
                  uint8_t update_type)
            {
                auto fields = std::make_shared<FieldArray>(11);

                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(POSITION) = std::make_shared<ConstTypeField<uint32_t>>(position);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(LSN) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
                fields->at(EXISTS) = std::make_shared<ConstTypeField<bool>>(exists);
                fields->at(NAME) = std::make_shared<ConstTypeField<std::string>>(name);
                fields->at(TYPE) = std::make_shared<ConstTypeField<uint8_t>>(type);
                fields->at(PG_TYPE) = std::make_shared<ConstTypeField<int32_t>>(pg_type);
                fields->at(NULLABLE) = std::make_shared<ConstTypeField<bool>>(nullable);
                if (default_value) {
                    fields->at(DEFAULT) = std::make_shared<ConstTypeField<std::string>>(*default_value);
                } else {
                    fields->at(DEFAULT) = std::make_shared<ConstNullField>(SchemaType::TEXT);
                }
                fields->at(UPDATE_TYPE) = std::make_shared<ConstTypeField<uint8_t>>(update_type);

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

    /**
     * Helper functions and constants for the table_stats table.
     */
    class TableStats {
    public:
        static constexpr uint32_t ID = 5;

        struct Data {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t XID = 1;
            static constexpr uint32_t ROW_COUNT = 2;

            static const std::vector<SchemaColumn> SCHEMA;

            static TuplePtr
            tuple(uint64_t table_id,
                  uint64_t xid,
                  uint64_t row_count)
            {
                auto fields = std::make_shared<FieldArray>(4);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(ROW_COUNT) = std::make_shared<ConstTypeField<uint64_t>>(row_count);
                return std::make_shared<FieldTuple>(fields, nullptr);
            }
        };

        struct Primary {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t XID = 1;

            static const std::vector<SchemaColumn> SCHEMA;
            static const std::vector<std::string> KEY;

            static TuplePtr
            key_tuple(uint64_t table_id, uint64_t xid) {
                auto key_fields = std::make_shared<FieldArray>(2);
                key_fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                key_fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                return std::make_shared<FieldTuple>(key_fields, nullptr);
            }
        };
    };

    /**
     * Helper functions and constants for the index_names table.
     */
    class IndexNames {
    public:
        enum class State {
            NOT_READY,
            READY,
            DELETED
        };
        static constexpr uint32_t ID = 6;

        struct Data {
            static constexpr uint32_t NAMESPACE = 0;
            static constexpr uint32_t NAME = 1;
            static constexpr uint32_t TABLE_ID = 2;
            static constexpr uint32_t INDEX_ID = 3;
            static constexpr uint32_t XID = 4;
            static constexpr uint32_t LSN = 5;
            static constexpr uint32_t STATE = 6;
            static constexpr uint32_t IS_UNIQUE = 7;

            static const std::vector<SchemaColumn> SCHEMA;

            static TuplePtr
            tuple(const std::string &schema,
                  const std::string &name,
                  uint64_t table_id,
                  uint64_t index_id,
                  uint64_t xid,
                  uint64_t lsn,
                  State state,
                  bool is_unique)
            {
                auto fields = std::make_shared<FieldArray>(8);
                fields->at(NAMESPACE) = std::make_shared<ConstTypeField<std::string>>(schema);
                fields->at(NAME) = std::make_shared<ConstTypeField<std::string>>(name);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(LSN) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
                fields->at(STATE) = std::make_shared<ConstTypeField<uint8_t>>(static_cast<uint8_t>(state));
                fields->at(IS_UNIQUE) = std::make_shared<ConstTypeField<bool>>(is_unique);
                return std::make_shared<FieldTuple>(fields, nullptr);
            }
        };

        struct Primary {
            static constexpr uint32_t TABLE_ID = 0;
            static constexpr uint32_t INDEX_ID = 1;
            static constexpr uint32_t XID = 2;
            static constexpr uint32_t LSN = 3;

            static const std::vector<SchemaColumn> SCHEMA;
            static const std::vector<std::string> KEY;

            static TuplePtr
            key_tuple(uint64_t table_id,
                      uint64_t index_id,
                      uint64_t xid,
                      uint64_t lsn)
            {
                auto fields = std::make_shared<FieldArray>(4);
                fields->at(TABLE_ID) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
                fields->at(INDEX_ID) = std::make_shared<ConstTypeField<uint64_t>>(index_id);
                fields->at(XID) = std::make_shared<ConstTypeField<uint64_t>>(xid);
                fields->at(LSN) = std::make_shared<ConstTypeField<uint64_t>>(lsn);
                return std::make_shared<FieldTuple>(fields, nullptr);
            }
        };
    };

}
