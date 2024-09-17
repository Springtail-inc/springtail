namespace cpp springtail.thrift.xid_mgr

typedef i64 xid_t

// status code and optional message
struct Status {
    1: StatusCode status,
    2: optional string message
}

// status code type
enum StatusCode {
    SUCCESS=0,
    ERROR=1
}

service ThriftXidMgr {
    Status ping(),

    // commit upto and including the provided xid
    Status commit_xid(1: i64 db_id, 2: xid_t xid, 3: bool has_schema_changes),

    // commit upto and including the provided xid
    Status record_ddl_change(1: i64 db_id, 2: xid_t xid),

    // get latest committed xid
    xid_t get_committed_xid(1: i64 db_id, 2: xid_t schema_xid)
}
