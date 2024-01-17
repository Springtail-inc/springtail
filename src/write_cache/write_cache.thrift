namespace cpp springtail.thrift

// table change operation type
enum TableChangeOpType {
    TRUNCATE_TABLE=1,
    SCHEMA_CHANGE=2
}

// status code type
enum StatusCode {
    SUCCESS=0,
    ERROR=1
}

// row object -- sorted on <xid,xid_seq>
// updates for same xid will overwrite
struct Row {
    1: i64 xid,
    2: i64 xid_seq,
    3: bool delete_flag,
    4: binary primary_key,
    5: optional binary data  // no data for deletes
}

// table change record
struct TableChange {
    1: i64 table_id,
    2: i64 xid,
    3: i64 xid_seq,
    4: TableChangeOpType op
}

// add a list of rows (or one)
struct AddRowsRequest {
    1: i64 table_id,
    2: i64 extent_id,
    3: list<Row> rows
}

// get a list of rows between range start xid exclusive to end xid inclusive
struct GetRowsRequest {
    1: i64 table_id,
    2: i64 extent_id,
    3: i64 start_xid,
    4: i64 end_xid,
    5: i64 cursor,
    6: i32 count
}

struct GetRowsResponse {
    1: i64 table_id,
    2: i64 extent_id,
    3: i64 cursor,
    4: list<Row> rows
}

// list dirty extents between range start xid exclusive and end xid inclusive
struct ListExtentsRequest {
    1: i64 table_id,
    2: i64 start_xid,
    3: i64 end_xid,
    4: i64 cursor,
    5: i32 count
}

// list of extents for a table_id; cursor is 0 if no more
struct ListExtentsResponse {
    1: i64 table_id,
    2: i64 cursor,
    3: list<i64> extent_ids;
}

struct ListTablesRequest {
    1: i64 start_xid,
    2: i64 end_xid,
    3: i32 count
}

struct ListTablesResponse {
    1: list<i64> table_ids;
}

// remove an extent and its associated rows from cached
// between start xid exclusive and end xid inclusive
struct EvictTableRequest {
    1: i64 table_id,
    3: i64 start_xid,
    4: i64 end_xid
}

// get list of tables changes between xid
struct GetTableChangeRequest {
    1: i64 table_id,
    2: i64 start_xid,
    3: i64 end_xid
}

struct GetTableChangeResponse {
    1: i64 table_id,
    2: list<TableChange> changes
}

// evict table changes between xid range
struct EvictTableChangesRequest {
    1: i64 table_id,
    2: i64 start_xid,
    3: i64 end_xid
}

// status code and optional message
struct Status {
    1: StatusCode status,
    2: optional string message
}

// list of rpc requests and responses
service ThriftWriteCache {
    Status ping(),
    Status add_rows(1: AddRowsRequest request),
    ListExtentsResponse list_extents(1: ListExtentsRequest request),
    GetRowsResponse get_rows(1: GetRowsRequest request),
    Status evict_table(1: EvictTableRequest request),
    Status add_table_change(1: TableChange change),
    GetTableChangeResponse get_table_changes(1: GetTableChangeRequest request),
    ListTablesResponse list_tables(1: ListTablesRequest request),
    Status evict_table_changes(1: EvictTableChangesRequest request)
}
