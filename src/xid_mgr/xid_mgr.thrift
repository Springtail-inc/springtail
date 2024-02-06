namespace cpp springtail.thrift

struct XidRange {
    1: i64 start_xid,
    2: i64 end_xid
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
    XidRange get_xid_range()
}