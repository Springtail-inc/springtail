#include <storage/system_tables.hh>

// note: these are hard-coded from the postgres type OIDs to avoid having to include all of the
// postgres headers here
constexpr int32_t BOOLOID = 16;
constexpr int32_t INT8OID = 20;
constexpr int32_t INT4OID = 23;
constexpr int32_t TEXTOID = 25;

namespace springtail::sys_tbl {

    // TableNames

    const std::vector<SchemaColumn> TableNames::Data::SCHEMA = {
        { "namespace", 0, SchemaType::TEXT, TEXTOID, false, 0 },
        { "name", 1, SchemaType::TEXT, TEXTOID, false, 1 },
        { "table_id", 2, SchemaType::UINT64, INT8OID, false },
        { "xid", 3, SchemaType::UINT64, INT8OID, false, 2 },
        { "lsn", 4, SchemaType::UINT64, INT8OID, false, 3 },
        { "exists", 5, SchemaType::BOOLEAN, BOOLOID, false }
    };

    const std::vector<SchemaColumn> TableNames::Primary::SCHEMA = {
        { "namespace", 0, SchemaType::TEXT, TEXTOID, false },
        { "name", 1, SchemaType::TEXT, TEXTOID, false },
        { "xid", 2, SchemaType::UINT64, INT8OID, false },
        { "lsn", 3, SchemaType::UINT64, INT8OID, false },
        { constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, INT8OID, false }
    };

    const std::vector<std::string> TableNames::Primary::KEY = {
        "namespace",
        "name",
        "xid",
        "lsn"
    };

    const std::vector<SchemaColumn> TableNames::Secondary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, INT8OID, false },
        { "xid", 1, SchemaType::UINT64, INT8OID, false },
        { "lsn", 2, SchemaType::UINT64, INT8OID, false }
    };

    const std::vector<std::string> TableNames::Secondary::KEY = {
        "table_id",
        "xid",
        "lsn"
    };

    // TableRoots

    const std::vector<SchemaColumn> TableRoots::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, INT8OID, false, 0 },
        { "index_id", 1, SchemaType::UINT64, INT8OID, false, 1 },
        { "xid", 2, SchemaType::UINT64, INT8OID, false, 2 },
        { "extent_id", 3, SchemaType::UINT64, INT8OID, false }
    };

    const std::vector<SchemaColumn> TableRoots::Primary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, INT8OID, false },
        { "index_id", 1, SchemaType::UINT64, INT8OID, false },
        { "xid", 2, SchemaType::UINT64, INT8OID, false },
        { constant::INDEX_EID_FIELD, 3, SchemaType::UINT64, INT8OID, false }
    };

    const std::vector<std::string> TableRoots::Primary::KEY = {
        "table_id",
        "index_id",
        "xid"
    };

    // Indexes

    const std::vector<SchemaColumn> Indexes::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, INT8OID, false, 0 },
        { "index_id", 1, SchemaType::UINT64, INT8OID, false, 1 },
        { "xid", 2, SchemaType::UINT64, INT8OID, false, 2 },
        { "position", 3, SchemaType::UINT32, INT4OID, false, 3 },
        { "column_id", 4, SchemaType::UINT32, INT4OID, false }
    };

    const std::vector<SchemaColumn> Indexes::Primary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, INT8OID, false },
        { "index_id", 1, SchemaType::UINT64, INT8OID, false },
        { "xid", 2, SchemaType::UINT64, INT8OID, false },
        { "position", 3, SchemaType::UINT32, INT4OID, false },
        { constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, INT8OID, false }
    };

    const std::vector<std::string> Indexes::Primary::KEY = {
        "table_id",
        "index_id",
        "xid",
        "position"
    };

    // Schemas

    const std::vector<SchemaColumn> Schemas::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, INT8OID, false, 0 },
        { "position", 1, SchemaType::UINT32, INT4OID, false, 1 },
        { "xid", 2, SchemaType::UINT64, INT8OID, false, 2 },
        { "lsn", 3, SchemaType::UINT64, INT8OID, false, 3 },
        { "exists", 4, SchemaType::BOOLEAN, BOOLOID, false },
        { "name", 5, SchemaType::TEXT, TEXTOID, false },
        { "type", 6, SchemaType::UINT8, INT8OID, false },
        { "pg_type", 7, SchemaType::INT32, INT4OID, false },
        { "nullable", 8, SchemaType::BOOLEAN, BOOLOID, false },
        { "default", 9, SchemaType::TEXT, TEXTOID, true },
        { "update_type", 10, SchemaType::UINT8, INT8OID, false }
    };

    const std::vector<SchemaColumn> Schemas::Primary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, INT8OID, false },
        { "position", 1, SchemaType::UINT32, INT4OID, false },
        { "xid", 2, SchemaType::UINT64, INT8OID, false },
        { "lsn", 3, SchemaType::UINT64, INT8OID, false },
        { constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, INT8OID, false }
    };

    const std::vector<std::string> Schemas::Primary::KEY = {
        "table_id",
        "position",
        "xid",
        "lsn"
    };

    // TableStats
    const std::vector<SchemaColumn> TableStats::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, INT8OID, false, 0 },
        { "xid", 1, SchemaType::UINT64, INT8OID, false, 1 },
        { "row_count", 2, SchemaType::UINT64, INT8OID, false }
    };

    const std::vector<SchemaColumn> TableStats::Primary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, INT8OID, false },
        { "xid", 1, SchemaType::UINT64, INT8OID, false },
        { constant::INDEX_EID_FIELD, 2, SchemaType::UINT64, INT8OID, false }
    };

    const std::vector<std::string> TableStats::Primary::KEY = {
        "table_id",
        "xid"
    };
}
