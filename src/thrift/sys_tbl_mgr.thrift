namespace cpp springtail.sys_tbl_mgr

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

struct DDLStatement {
    1: string statement
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

struct TableInfo {
    1: i64 id,
    2: string schema,
    3: string name,
    4: list<TableColumn> columns
}

struct TableRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 lsn,
    4: TableInfo table
}

struct DropTableRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 lsn,
    4: i64 table_id,
    5: string schema,
    6: string name
}

struct TableStats {
    1: i64 row_count
}

struct UpdateRootsRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 table_id,
    4: list<i64> roots,
    5: TableStats stats
}

struct FinalizeRequest {
    1: i64 db_id,
    2: i64 xid
}

struct GetRootsRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 table_id
}

struct GetRootsResponse {
    1: list<i64> roots,
    2: TableStats stats
}

struct GetSchemaRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 xid,
    4: i64 lsn
}

struct GetTargetSchemaRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 access_xid,
    4: i64 access_lsn,
    5: i64 target_xid,
    6: i64 target_lsn
}

struct ColumnHistory {
    1: i64 xid,
    2: i64 lsn,
    3: bool exists,
    4: i8 update_type
    5: TableColumn column
}

struct GetSchemaResponse {
    1: list<TableColumn> columns,
    2: list<ColumnHistory> history
}

struct ExistsRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 xid,
    4: i64 lsn
}

// the interface for managing the system tables
service Service {
    Status ping(),

    // creates a new data table at the given xid/lsn
    DDLStatement create_table(1: TableRequest request),

    // alters the metadata of an existing data table at the given xid/lsn
    DDLStatement alter_table(1: TableRequest request),

    // drops an existing data table at the given xid/lsn
    DDLStatement drop_table(1: DropTableRequest request),

    // update the index root pointers and stats for a given table at a given xid
    Status update_roots(1: UpdateRootsRequest request),

    // finalize the system tables at a given XID
    Status finalize(1: FinalizeRequest request),

    // retrieve the root pointers and stats for a given table at a given xid
    GetRootsResponse get_roots(1: GetRootsRequest request),

    // retrieve the schema information for a given table at a given xid/lsn
    GetSchemaResponse get_schema(1: GetSchemaRequest request),

    // retrieve the schema information for a given table at a given xid/lsn with changes up to the target xid/lsn
    GetSchemaResponse get_target_schema(1: GetTargetSchemaRequest request),

    // checks if the table exists at a given xid/lsn
    bool exists(1: ExistsRequest request)
}
