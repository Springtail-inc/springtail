namespace cpp springtail.thrift

typedef i64 xid_t

struct XidRange {
    1: xid_t start_xid,
    2: xid_t end_xid
}

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
    // get an unused xid range
    XidRange get_xid_range(1: xid_t last_xid),

    // commit upto and including the provided xid
    Status commit_xid(1: xid_t xid),

    // get latest committed xid
    xid_t get_committed_xid()
}