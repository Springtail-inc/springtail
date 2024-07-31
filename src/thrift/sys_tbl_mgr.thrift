namespace cpp springtail.thrift.sys_tbl_mgr

// status code and optional message
struct Status {
    1: StatusCode status
    2: optional string message
}

// status code type
enum StatusCode {
    SUCCESS=0,
    ERROR=1
}

struct TableColumn {
    1: string name,
    2: i8 type,
    3: i32 pg_type,
    4: i32 position,
    5: bool is_nullable,
    6: bool is_generated,
    7: optional i32 pk_position,
    8: optional string default_value,
}

struct TableMetadata {
    1: i64 id,
    2: string schema,
    3: string name,
    4: list<TableColumn> columns
}

struct TableRequest {
    1: i64 xid,
    2: i64 lsn,
    3: TableMetadata table
}

struct DropTableRequest {
    1: i64 xid,
    2: i64 lsn,
    3: i64 table_id,
    4: string schema,
    5: string name
}

struct TableStats {
    1: i64 row_count
}

struct UpdateRootsRequest {
    1: i64 xid,
    2: i64 table_id,
    3: list<i64> roots,
    4: TableStats stats
}

struct FinalizeRequest {
    1: i64 xid
}

struct GetRootsRequest {
    1: i64 xid,
    2: i64 table_id
}

struct GetRootsResponse {
    1: list<i64> roots,
    2: TableStats stats
}

struct GetSchemaInfoRequest {
    1: i64 xid,
    2: i64 lsn,
    3: i64 table_id
}

struct ColumnHistory {
    1: i64 xid,
    2: i64 lsn,
    3: bool exists,
    4: i8 update_type
    5: TableColumn column
}

struct GetSchemaInfoResponse {
    1: list<TableColumn> columns,
    2: list<ColumnHistory> history
}

// the interface for managing the system tables
service ThriftSysTblMgr {
    Status ping(),

    // creates a new data table at the given xid/lsn
    Status create_table(1: TableRequest request),

    // alters the metadata of an existing data table at the given xid/lsn
    Status alter_table(1: TableRequest request),

    // drops an existing data table at the given xid/lsn
    Status drop_table(1: DropTableRequest request),

    // update the index root pointers and stats for a given table at a given xid
    Status update_roots(1: UpdateRootsRequest request),

    // finalize the system tables at a given XID
    Status finalize(1: FinalizeRequest request),

    // retrieve the root pointers and stats for a given table at a given xid
    GetRootsResponse get_roots(1: GetRootsRequest request),

    // retrieve the schema information for a given table at a given xid/lsn
    GetSchemaInfoResponse get_schema_info(1: GetSchemaInfoRequest request)
}
