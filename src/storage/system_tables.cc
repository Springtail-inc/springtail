#include <storage/system_tables.hh>

namespace springtail::sys_tbl {

    // TableNames

    const std::vector<SchemaColumn> TableNames::Data::SCHEMA = {
        { "namespace", 0, SchemaType::TEXT, false, 0 },
        { "name", 1, SchemaType::TEXT, false, 1 },
        { "table_id", 2, SchemaType::UINT64, false },
        { "xid", 3, SchemaType::UINT64, false, 2 },
        { "lsn", 4, SchemaType::UINT64, false, 3 },
        { "exists", 5, SchemaType::BOOLEAN, false }
    };

    const std::vector<SchemaColumn> TableNames::Primary::SCHEMA = {
        { "namespace", 0, SchemaType::TEXT, false },
        { "name", 1, SchemaType::TEXT, false },
        { "xid", 2, SchemaType::UINT64, false },
        { "lsn", 3, SchemaType::UINT64, false },
        { constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, false }
    };

    const std::vector<std::string> TableNames::Primary::KEY = {
        "namespace",
        "name",
        "xid",
        "lsn"
    };

    const std::vector<SchemaColumn> TableNames::Secondary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "xid", 1, SchemaType::UINT64, false },
        { "lsn", 2, SchemaType::UINT64, false }
        // { constant::INDEX_EID_FIELD, 3, SchemaType::UINT64, false },
        // { constant::INDEX_RID_FIELD, 4, SchemaType::UINT32, false }
    };

    const std::vector<std::string> TableNames::Secondary::KEY = {
        "table_id",
        "xid",
        "lsn"
        // constant::INDEX_EID_FIELD,
        // constant::INDEX_RID_FIELD
    };

    // TableRoots

    const std::vector<SchemaColumn> TableRoots::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false, 0 },
        { "index_id", 1, SchemaType::UINT64, false, 1 },
        { "xid", 2, SchemaType::UINT64, false, 2 },
        { "extent_id", 3, SchemaType::UINT64, false }
    };

    const std::vector<SchemaColumn> TableRoots::Primary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "index_id", 1, SchemaType::UINT64, false },
        { "xid", 2, SchemaType::UINT64, false },
        { constant::INDEX_EID_FIELD, 3, SchemaType::UINT64, false }
    };

    const std::vector<std::string> TableRoots::Primary::KEY = {
        "table_id",
        "index_id",
        "xid"
    };

    // Indexes

    const std::vector<SchemaColumn> Indexes::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false, 0 },
        { "index_id", 1, SchemaType::UINT64, false, 1 },
        { "xid", 2, SchemaType::UINT64, false, 2 },
        { "position", 3, SchemaType::UINT32, false, 3 },
        { "column_id", 4, SchemaType::UINT32, false }
    };

    const std::vector<SchemaColumn> Indexes::Primary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "index_id", 1, SchemaType::UINT64, false },
        { "xid", 2, SchemaType::UINT64, false },
        { "position", 3, SchemaType::UINT32, false },
        { constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, false }
    };

    const std::vector<std::string> Indexes::Primary::KEY = {
        "table_id",
        "index_id",
        "xid",
        "position"
    };

    // Schemas

    const std::vector<SchemaColumn> Schemas::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false, 0 },
        { "position", 1, SchemaType::UINT32, false, 1 },
        { "xid", 2, SchemaType::UINT64, false, 2 },
        { "lsn", 3, SchemaType::UINT64, false, 3 },
        { "exists", 4, SchemaType::BOOLEAN, false },
        { "name", 5, SchemaType::TEXT, false },
        { "type", 6, SchemaType::UINT8, false },
        { "nullable", 7, SchemaType::BOOLEAN, false },
        { "default", 8, SchemaType::TEXT, true },
        { "update_type", 9, SchemaType::UINT8, false }
    };

    const std::vector<SchemaColumn> Schemas::Primary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "position", 1, SchemaType::UINT32, false },
        { "xid", 2, SchemaType::UINT64, false },
        { "lsn", 3, SchemaType::UINT64, false },
        { constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, false }
    };

    const std::vector<std::string> Schemas::Primary::KEY = {
        "table_id",
        "position",
        "xid",
        "lsn"
    };
}
