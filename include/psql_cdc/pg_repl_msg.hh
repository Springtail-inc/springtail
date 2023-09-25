#pragma once

#include <psql_cdc/pg_types.hh>

/* Postgres message types, see decode functions in
 * PgReplMsg.cc for formats */
struct MsgTupleDataColumn {
    char type;  // 'n' NULL, 'u' unchanged TOAST, 't' text formatted, 'b' binary
    int32_t data_len;
    const char *data;
};

struct MsgTupleData {
    int16_t num_columns;
    std::vector<MsgTupleDataColumn> tuple_data;
};

struct MsgKeepAlive {
    LSN_t wal_end;
    bool response_requested;
};

struct MsgBegin {
    int32_t xid;
    LSN_t xact_lsn;
    int64_t commit_ts;
};

struct MsgCommit {
    LSN_t commit_lsn;
    LSN_t xact_lsn;
    int64_t commit_ts;
};

struct MsgOrigin {
    LSN_t commit_lsn;
    const char *name;
};

struct MsgCopy {
    LSN_t wal_start;
    LSN_t wal_end;
};

struct MsgTruncate {
    int32_t num_rels;
    int8_t options; // 1 for cascade; 2 for restart identity
    std::vector<int32_t> rel_ids;
};

struct MsgType {
    int32_t xid;
    int32_t oid;
    const char *namespace_str;
    const char *data_type_str;
};

struct MsgMessage {
    int32_t xid;
    int8_t flags;
    LSN_t lsn;
    const char *prefix_str;    // null terminated string
    int32_t data_len;
    const char *data;
};

struct MsgDelete {
    int32_t rel_id;
    char type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
    MsgTupleData tuple;
};

struct MsgInsert {
    int32_t rel_id;
    char new_type;
    MsgTupleData new_tuple;
};

struct MsgUpdate {
    int32_t rel_id;
    char old_type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
    MsgTupleData old_tuple;
    char new_type; // 'N' tuple data is new
    MsgTupleData new_tuple;
};

struct MsgRelColumn {
    int8_t flags;
    const char *column_name;
    int32_t data_type_id;
    int32_t type_modifier;
};

struct MsgRelation {
    int32_t rel_id;
    const char *namespace_str;
    const char *rel_name_str;
    int8_t identity;
    int16_t num_columns;
    std::vector<MsgRelColumn> columns;
};

union PgReplMsgDecodedUnion {
    PgReplMsgDecodedUnion() {}
    ~PgReplMsgDecodedUnion() {}

    MsgKeepAlive keep_alive;
    MsgBegin begin;
    MsgCommit commit;
    MsgOrigin origin;
    MsgCopy copy;
    MsgMessage message;
    MsgType type;
    MsgTruncate truncate;
    MsgDelete delete_msg;
    MsgUpdate update;
    MsgInsert insert;
    MsgRelation relation;
};

enum PgReplMsgType {
    INVALID, COPY_HDR, KEEP_ALIVE, BEGIN, COMMIT, RELATION, INSERT, DELETE, UPDATE, TRUNCATE,
    ORIGIN, MESSAGE, TYPE,
    // version 2
    STREAM_START, STREAM_STOP, STREAM_COMMIT, STREAM_ABORT
};

struct PgReplMsgDecoded {
    PgReplMsgDecodedUnion msg;
    PgReplMsgType msg_type;    // type defining union member
};

class PgReplMsg
{

private:
    // Proto V1; message flags, first byte
    static const char MSG_BEGIN = 'B';
    static const char MSG_COMMIT = 'C';
    static const char MSG_RELATION = 'R';
    static const char MSG_INSERT = 'I';
    static const char MSG_UPDATE = 'U';
    static const char MSG_DELETE = 'D';
    static const char MSG_TRUNCATE = 'T';
    static const char MSG_ORIGIN = 'O';
    static const char MSG_MESSAGE = 'M';
    static const char MSG_TYPE = 'Y';
    // Proto V2
    static const char MSG_STREAM_START = 'S';
    static const char MSG_STREAM_STOP = 'E';
    static const char MSG_STREAM_COMMIT = 'c';
    static const char MSG_STREAM_ABORT = 'A';

    /** Protocol version */
    int _proto_version;

    /** Internal buffer to decode messages out of, set by setBuffer() */
    const char *_buffer = nullptr;
    int _buffer_length = 0;

    /** Message struct used for returning decoded message */
    PgReplMsgDecoded _decoded_msg;

    /**
     * @brief Initialize message to empty/invalid message
     */
    inline void initMsg() {
        _decoded_msg.msg_type = PgReplMsgType::INVALID;
    }

    static int decodeKeepAlive(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeBegin(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeCommit(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeRelation(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeInsert(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeUpdate(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeDelete(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeTruncate(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeOrigin(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeMessage(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeCopyData(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeXlogHeader(const char *buffer, int length, PgReplMsgDecoded &msg);
    static int decodeType(const char *buffer, int length, PgReplMsgDecoded &msg);

public:

    PgReplMsg(int proto_version);

    void setBuffer(const char *buffer, int length);

    bool hasNextMsg();

    const PgReplMsgDecoded &decodeNextMsg();
};


