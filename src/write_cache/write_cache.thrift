namespace cpp springtail.thrift

enum RowOpType 
{
    INSERT=0,
    UPDATE=1,
    DELETE=2
}

enum StatusCode
{
    SUCCESS=0,
    ERROR=1
}

struct Row
{
    1:  i64 table_id,
    2:  i64 extent_id,
    3:  binary primary_key,
    4:  binary data
}

struct AddRowRequest
{
    1: RowOpType op,
    2: i64 xid,
    3: list<Row> rows
}

struct Status
{
    1: StatusCode status,
    2: optional string message
}

service ThriftWriteCache
{
    Status ping(),
    Status addRows(1: AddRowRequest request)
}
