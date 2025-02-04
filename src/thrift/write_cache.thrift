namespace cpp springtail.thrift.write_cache

// status code type
enum StatusCode {
    SUCCESS=0,
    ERROR=1
}

// Extent object
struct Extent {
    1: i64 xid,
    2: i64 xid_seq,
    3: binary data
}

// get a list of extents for a springtail xid
struct GetExtentsRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 xid,
    4: i64 cursor,
    5: i32 count
}

struct GetExtentsResponse {
    1: i64 table_id,
    2: i64 cursor,
    3: list<Extent> extents,
    // Postgres timestamp in microseconds since 2000-01-01
    4: i64 commit_ts
}

// list of tables for a db_id; cursor is 0 if no more
struct ListTablesRequest {
    1: i64 db_id,
    2: i64 xid,
    3: i64 cursor,
    4: i32 count
}

struct ListTablesResponse {
    1: i64 cursor,
    2: list<i64> table_ids
}

// remove all extents for a table_id and springtail xid
struct EvictTableRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 xid
}

// remove all data for a springtail xid
struct EvictXidRequest {
    1: i64 db_id,
    2: i64 xid
}

// ExtentMapper request structures

struct AddMappingRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 target_xid,
    4: i64 old_eid,
    5: list<i64> new_eids
}

struct SetLookupRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 target_xid,
    4: i64 extent_id
}

struct ForwardMapRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 target_xid,
    4: i64 extent_id
}

struct ReverseMapRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 access_xid,
    4: i64 target_xid,
    5: i64 extent_id
}

struct ExpireMapRequest {
    1: i64 db_id,
    2: i64 table_id,
    3: i64 commit_xid
}

struct ExtentMapResponse {
    1: list<i64> extent_ids
}

// status code and optional message
struct Status {
    1: StatusCode status,
    2: optional string message
}


// list of rpc requests and responses
service ThriftWriteCache {
    // core write cache functions
    Status ping(),
    GetExtentsResponse get_extents(1: GetExtentsRequest request),
    ListTablesResponse list_tables(1: ListTablesRequest request),
    Status evict_table(1: EvictTableRequest request),
    Status evict_xid(1: EvictXidRequest request),

    // extent mapper functions
    Status add_mapping(1: AddMappingRequest request),
    Status set_lookup(1: SetLookupRequest request),
    ExtentMapResponse forward_map(1: ForwardMapRequest request),
    ExtentMapResponse reverse_map(1: ReverseMapRequest request),
    Status expire_map(1: ExpireMapRequest request)
}
