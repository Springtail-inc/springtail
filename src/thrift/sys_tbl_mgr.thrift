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
    2: string namespace_name,
    3: string name,
    4: list<TableColumn> columns
}

struct TableRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 lsn,
    4: TableInfo table,
    5: i64 snapshot_xid
}

struct DropTableRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 lsn,
    4: i64 table_id,
    5: string namespace_name,
    6: string name
}

struct NamespaceRequest {
    1: i64 db_id,
    2: i64 namespace_id,
    3: string name,
    4: i64 xid,
    5: i64 lsn
}

struct DropNamespaceRequest {
    1: i64 db_id,
    2: i64 namespace_id,
    3: i64 xid,
    4: i64 lsn
}

struct TableStats {
    1: i64 row_count
}

struct RootInfo {
    1: i64 index_id,
    2: i64 extent_id,
}

struct IndexColumn {
    1: string name,
    2: i32 position,
    3: i32 idx_position
}

struct IndexInfo {
    1: i64 id,
    2: string namespace_name,
    3: string name,
    4: bool is_unique,
    5: i64 table_id,
    6: string table_name,
    7: i8 state,
    8: list<IndexColumn> columns
}

struct IndexRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 lsn,
    4: IndexInfo index,
}

struct SetIndexStateRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 lsn,
    4: i64 table_id,
    5: i64 index_id,
    6: i8 state,
}

struct DropIndexRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 lsn,
    4: i64 index_id,
    5: string namespace_name,
    6: string name
}

struct GetIndexInfoRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 lsn,
    4: i64 index_id,
    5: optional i64 table_id
}

struct UpdateRootsRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 table_id,
    4: list<RootInfo> roots,
    5: TableStats stats,
    6: i64 snapshot_xid
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
    1: list<RootInfo> roots,
    2: TableStats stats,
    3: i64 snapshot_xid
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
    5: optional TableColumn column
    6: optional IndexColumn index_column
}

struct GetSchemaResponse {
    1: map<i32, TableColumn> columns,
    2: list<ColumnHistory> history,
    3: list<IndexInfo> indexes,
    4: i64 access_xid_start,
    5: i64 access_lsn_start,
    6: i64 access_xid_end,
    7: i64 access_lsn_end,
    8: i64 target_xid_start,
    9: i64 target_lsn_start,
    10: i64 target_xid_end,
    11: i64 target_lsn_end
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

    // creates a new index at the given xid/lsn
    DDLStatement create_index(1: IndexRequest request),

    // drops an existing index at the given xid/lsn
    DDLStatement drop_index(1: DropIndexRequest request),

    // set the index state at the given xid/lsn
    Status set_index_state(1: SetIndexStateRequest request),

    // get the index info at the given xid/lsn
    IndexInfo get_index_info(1: GetIndexInfoRequest request),

    // creates a new data table at the given xid/lsn
    DDLStatement create_table(1: TableRequest request),

    // alters the metadata of an existing data table at the given xid/lsn
    DDLStatement alter_table(1: TableRequest request),

    // drops an existing data table at the given xid/lsn
    DDLStatement drop_table(1: DropTableRequest request),

    // create a namespace at a given xid/lsn
    DDLStatement create_namespace(1: NamespaceRequest request),

    // rename a namespace at a given xid/lsn
    DDLStatement alter_namespace(1: NamespaceRequest request),

    // drop a namespace at a given xid/lsn
    DDLStatement drop_namespace(1: DropNamespaceRequest request),

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

    // performs a drop + create + update_roots as a single operation
    // to support swapping a newly synced table into place
    DDLStatement swap_sync_table(1: NamespaceRequest namespace_req,
				 2: TableRequest create_req,
				 3: list<IndexRequest> index_reqs,
				 4: UpdateRootsRequest roots_req);
}
