namespace cpp springtail.thrift

// row operation type
enum RowOpType {
    INSERT=0,
    UPDATE=1,
    DELETE=2
}

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
    1: i64 table_id,
    2: i64 extent_id,
    3: i64 xid,
    4: i64 xid_seq,
    5: binary data,
    6: optional binary primary_key,
    7: optional bool delete_flag    // only in response
}

struct TableChange {
    1: i64 table_id, 
    2: i64 xid,
    3: i64 xid_seq,
    4: TableChangeOpType op
}

// extent descriptor
struct Extent {
    1: i64 table_id,
    2: i64 extent_id
}

// add a list of rows (or one)
struct AddRowRequest {
    1: RowOpType op,
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
    3: list<Extent> extents
}

struct ListTablesRequest {
    1: i64 start_xid,
    2: i64 end_xid,
    3: i64 cursor,
    4: i32 count
}

struct ListTablesResponse {
    1: i64 cursor,
    2: list<i64> table_ids;
}

// remove an extent and its associated rows from cached
// between start xid exclusive and end xid inclusive
struct EvictExtentRequest {
    1: i64 table_id,
    2: i64 extent_id,
    3: i64 start_xid,
    4: i64 end_xid
}

// get list of tables changes between xid
struct GetTableChangeRequest {
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
    Status add_rows(1: AddRowRequest request),
    ListExtentsResponse list_extents(1: ListExtentsRequest request),
    GetRowsResponse get_rows(1: GetRowsRequest request),
    Status evict_extent(1: EvictExtentRequest request),
    Status add_table_changes(1: list<TableChange> changes),
    list<TableChange> get_table_changes(1: GetTableChangeRequest request),
    ListTablesResponse list_tables(1: ListTablesRequest request)
}
