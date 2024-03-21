#include <storage/system_tables.hh>

namespace springtail::sys_tbl {

    // TableNames

    TableNames::Data::SCHEMA = {
        { "namespace", 0, SchemaType::TEXT, false }
        { "name", 1, SchemaType::TEXT, false }
        { "table_id", 2, SchemaType::UINT64, false },
        { "xid", 3, SchemaType::UINT64, false },
        { "lsn", 4, SchemaType::UINT64, false },
        { "exists", 5, SchemaType::BOOLEAN, false },
    };

    TableNames::Primary::SCHEMA = {
        { "namespace", 0, SchemaType::TEXT, false }
        { "name", 1, SchemaType::TEXT, false }
        { "xid", 2, SchemaType::UINT64, false },
        { "lsn", 3, SchemaType::UINT64, false },
        { "extent_id", 4, SchemaType::UINT64, false },
    };

    TableNames::Secondary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "xid", 1, SchemaType::UINT64, false },
        { "lsn", 2, SchemaType::UINT64, false },
        { "extent_id", 3, SchemaType::UINT64, false },
        { "row_id", 4, SchemaType::UINT32, false },
    };

    // TableRoots

    TableRoots::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "index_id", 1, SchemaType::UINT64, false },
        { "xid", 2, SchemaType::UINT64, false },
        { "extent_id", 3, SchemaType::UINT64, false }
    };

    TableRoots::Primary::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "index_id", 1, SchemaType::UINT64, false },
        { "xid", 2, SchemaType::UINT64, false },
        { "extent_id", 3, SchemaType::UINT64, false }
    };

    // Indexes

    Indexes::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "index_id", 1, SchemaType::UINT64, false },
        { "xid", 2, SchemaType::UINT64, false },
        { "position", 3, SchemaType::UINT32, false },
        { "column_id", 4, SchemaType::UINT32, false }
    };

    // Schemas

    Schemas::Data::SCHEMA = {
        { "table_id", 0, SchemaType::UINT64, false },
        { "position", 1, SchemaType::UINT32, false },
        { "xid", 2, SchemaType::UINT64, false },
        { "lsn", 3, SchemaType::UINT64, false },
        { "exists", 4, SchemaType::BOOLEAN, false },
        { "name", 5, SchemaType::TEXT, false },
        { "type", 6, SchemaType::UINT8, false },
        { "nullable", 7, SchemaType::BOOLEAN, false },
        { "default", 8, SchemaType::TEXT, true },
        { "update_type", 9, SchemaType::UINT8, false }
    };
}
