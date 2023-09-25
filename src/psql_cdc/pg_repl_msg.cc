#include <psql_cdc/pg_repl_msg.hh>


PgReplMsg::PgReplMsg(int proto_version)
  : _proto_version(proto_version) {}


/**
 * @brief Initialize message to empty/invalid message
 */
void PgReplMsg::setBuffer(const char *buffer, int length)
{
    _buffer = buffer;
    _buffer_length = length;
    initMsg();
}


/**
 * @brief Does more data exist to process
 *
 * @return true if more data exists, false otherwise
 */
bool PgReplMsg::hasNextMsg()
{
    return (_buffer_length > 0);
}


/**
 * @brief Decode message in buffer, after xlog header
 * @return number of bytes consumed
 */
const PgReplMsgDecoded &PgReplMsg::decodeNextMsg()
{
    // first byte is opcode
    // then int32 length (usually)
    char msg_type = _buffer[0];
    int pos = 0;

    initMsg();

    switch(msg_type) {

        // V1 Protocol
        case MSG_BEGIN: // begin
            pos = decodeBegin(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_COMMIT: // commit
            pos = decodeCommit(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_RELATION: // relation
            pos = decodeRelation(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_INSERT: // insert
            pos = decodeInsert(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_UPDATE: // update
            pos = decodeUpdate(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_DELETE: // delete
            pos = decodeDelete(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_TRUNCATE: // truncate
            pos = decodeTruncate(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_ORIGIN: // origin
            pos = decodeOrigin(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_MESSAGE: // message
            pos = decodeMessage(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_TYPE: // type
            pos = decodeType(_buffer, _buffer_length, _decoded_msg);
            break;

        case MSG_STREAM_START:
        case MSG_STREAM_STOP:
        case MSG_STREAM_COMMIT:
        case MSG_STREAM_ABORT:
            std::cerr << "Streaming not supported";
            return _decoded_msg;

        default: // unknown/unhandled
            std::cerr << "Unknown opcode to decode: " << msg_type;
            return _decoded_msg;
    }

    // sanity check
    if (pos > _buffer_length) {
        std::cerr << "Buffer overrun in decode";
        initMsg();
        return _decoded_msg;
    }

    _buffer_length -= pos;
    _buffer += pos;

    return _decoded_msg;
}


/**
 * @brief Decode tuple data within a message
 *
 * @param buffer pointer to a buffer
 * @param length length of data in buffer
 * @param tuple tuple struct to hold output
 * @return number of bytes consumed
 */
int decodeTuple(const char *buffer, int length, MsgTupleData &tuple)
{
    int pos = 0;

    int16_t num_columns = recvint16(&buffer[pos]);
    pos += 2;

    tuple.num_columns = num_columns;

    for (int i = 0; i < num_columns; i++) {
        char type = buffer[pos];
        pos += 1;

        int32_t data_len = recvint32(&buffer[pos]);
        pos += 4;

        const char *data = &buffer[pos];
        pos += data_len;

        MsgTupleDataColumn column;
        column.type = type;
        column.data_len = data_len;
        column.data = data;

        tuple.tuple_data.push_back(column);
    }

    return pos;
}


/**
 * @brief [brief description]
 * @details [long description]
 *
 * @param buffer [description]
 * @param length [description]
 * @param msg [description]
 * @return [description]
 */
int PgReplMsg::decodeMessage(const char *buffer, int length,
                             PgReplMsgDecoded &msg)
{
    /*
        Byte1('M') Identifies the message as a logical decoding message.
        Int32 (TransactionId) Xid of the transaction (only present for streamed transactions).
               This field is available since protocol version 2.
        Int8 Flags; Either 0 for no flags or 1 if the logical decoding message is transactional.
        Int64 (XLogRecPtr) The LSN of the logical decoding message.
        String The prefix of the logical decoding message.
        Int32 Length of the content.
        Byten The content of the logical decoding message.
    */

    int pos = 1;

/*
    int32_t xid = recvint32(&buffer[pos]);  // only version 2
    pos += 4;
*/
    int8_t flags = (int8_t)buffer[pos];
    pos += 1;

    LSN_t lsn = recvint64(&buffer[pos]);
    pos += 8;

    int string_len = std::strlen(&buffer[pos]);
    const char *prefix_str = &buffer[pos];
    pos += string_len;

    int32_t data_len = recvint32(&buffer[pos]);
    pos += 4;

    const char *data = &buffer[pos];
    pos += data_len;

    msg.msg_type = PgReplMsgType::MESSAGE;
    msg.msg.message.flags = flags;
    msg.msg.message.lsn = lsn;
    msg.msg.message.prefix_str = prefix_str;
    msg.msg.message.data_len = data_len;
    msg.msg.message.data = data;

    return pos;
}


int PgReplMsg::decodeOrigin(const char *buffer, int length,
                          PgReplMsgDecoded &msg)
{
    /*
        Byte1('O') Identifies the message as an origin message.
        Int64 (XLogRecPtr) The LSN of the commit on the origin server.
        String Name of the origin.

        Note that there can be multiple Origin messages inside a single transaction.
    */
    int pos = 1;

    LSN_t lsn = recvint64(&buffer[pos]);
    pos += 8;

    msg.msg_type = PgReplMsgType::ORIGIN;
    msg.msg.origin.commit_lsn = lsn;
    msg.msg.origin.name = &buffer[pos];

    return pos;
}


int PgReplMsg::decodeBegin(const char *buffer, int length,
                           PgReplMsgDecoded &msg)
{
    /*
        Byte1('B') Identifies the message as a begin message.
        Int64 The final LSN of the transaction.
        Int64 Commit timestamp of the transaction. Number of microseconds since Y2K
        Int32 Xid of the transaction.
    */
    int pos = 1;

    LSN_t lsn = recvint64(&buffer[pos]);
    pos += 8;

    int64_t ts = recvint64(&buffer[pos]);
    pos += 8;

    int32_t xid = recvint32(&buffer[pos]);
    pos += 4;

    msg.msg_type = PgReplMsgType::BEGIN;
    msg.msg.begin.xid = xid;
    msg.msg.begin.xact_lsn = lsn;
    msg.msg.begin.commit_ts = ts;

    return pos;
}


int PgReplMsg::decodeCommit(const char *buffer, int length,
                            PgReplMsgDecoded &msg)
{
    /*
        Byte1('C') Identifies the message as a commit message.
        Int8 Flags; currently unused (must be 0).
        Int64 The LSN of the commit.
        Int64 The end LSN of the transaction.
        Int64 Commit timestamp of the transaction. Number of microseconds since Y2K
    */
    int pos = 1;
    // skip flags
    pos += 1;

    LSN_t commit_lsn = recvint64(&buffer[pos]);
    pos += 8;

    LSN_t xact_lsn = recvint64(&buffer[pos]);
    pos += 8;

    int64_t ts = recvint64(&buffer[pos]);
    pos += 8;

    msg.msg_type = PgReplMsgType::COMMIT;
    msg.msg.commit.commit_lsn = commit_lsn;
    msg.msg.commit.xact_lsn = xact_lsn;
    msg.msg.commit.commit_ts = ts;

    return pos;
}


int PgReplMsg::decodeRelation(const char *buffer, int length,
                              PgReplMsgDecoded &msg)
{
    /*
        Byte1('R')  Identifies the message as a relation message.
        Int32 Xid of the transaction (only present for streamed transactions).
              This field is available since protocol version 2.
        Int32 ID of the relation.
        String Namespace (empty string for pg_catalog).
        String Relation name.
        Int8 Replica identity setting for the relation (same as relreplident in pg_class).
          # select relreplident from pg_class where relname = 'test_table';
          # from the documentation and looking at the tables this is not int8 but a single character
          # background: https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY
        Int16 Number of columns.
        Next, the following message part appears for each column (except generated columns):
            Int8 Flags for the column. Currently can be either 0 for no flags or 1 which marks
                 the column as part of the key.
            String Name of the column.
            Int32 ID of the column's data type.
            Int32 Type modifier of the column (atttypmod).
    */

    int pos = 1;

    /*
    int32_t xid = recvint32(&buffer[pos]);     // only present in v2
    pos += 4;
    */

    int32_t rel_id = recvint32(&buffer[pos]);
    pos += 4;

    const char *namespace_str = &buffer[pos];
    pos += std::strlen(&buffer[pos]);

    const char *rel_name = &buffer[pos];
    pos += std::strlen(&buffer[pos]);

    int8_t identity = (int8_t)buffer[pos];
    pos += 1;

    int16_t num_columns = recvint16(&buffer[pos]);
    pos += 2;

    for (int i = 0; i < num_columns; i++) {
        MsgRelColumn column;

        column.flags = (int8_t)buffer[pos]; // 0 no flags; 1 key
        pos += 1;

        column.column_name = &buffer[pos];
        pos += std::strlen(column.column_name);

        column.data_type_id = recvint32(&buffer[pos]);
        pos += 4;

        column.type_modifier = recvint32(&buffer[pos]);
        pos += 4;

        msg.msg.relation.columns.push_back(column);
    }

    msg.msg_type = PgReplMsgType::RELATION;
    msg.msg.relation.rel_id = rel_id;
    msg.msg.relation.namespace_str = namespace_str;
    msg.msg.relation.rel_name_str = rel_name;
    msg.msg.relation.identity = identity;
    msg.msg.relation.num_columns = num_columns;

    return pos;
}


int PgReplMsg::decodeInsert(const char *buffer, int length,
                            PgReplMsgDecoded &msg)
{
    /*
        Byte1('I')  Identifies the message as an insert message.
        Int32 ID of the relation corresponding to the ID in the relation message
        Int32 XID present since version 2 (PG14)
        Byte1('N') Identifies the following TupleData message as a new tuple.
        TupleData TupleData message part representing the contents of new tuple.
    */
    int pos = 1;

    /*
    int32_t xid = recvint32(&buffer[pos]);     // only present in v2
    pos += 4;
    */

    int32_t rel_id = recvint32(&buffer[pos]);
    pos += 4;

    char new_type = buffer[pos]; // should be 'N'
    if (new_type == 'N') {
        pos += 1;
    } else {
        // no type present
        // XXX check if this means no tuple to decode...
        new_type = '\0';
    }

    pos += decodeTuple(&buffer[pos], length - pos, msg.msg.insert.new_tuple);

    msg.msg_type = PgReplMsgType::INSERT;
    msg.msg.insert.rel_id = rel_id;
    msg.msg.insert.new_type = new_type;

    return pos;
}


int PgReplMsg::decodeUpdate(const char *buffer, int length,
                            PgReplMsgDecoded &msg)
{
    /*
        Byte1('U')      Identifies the message as an update message.
        Int32           XID present since version 2 (PG14)
        Int32           ID of the relation corresponding to the ID in the relation message.
        Byte1('K')      Identifies the following TupleData submessage as a key.
                        This field is optional and is only present if the update changed data in
                        any of the column(s) that are part of the REPLICA IDENTITY index.
        Byte1('O')      Identifies the following TupleData submessage as an old tuple.
                        This field is optional and is only present if table in which the update
                        happened has REPLICA IDENTITY set to FULL.
        TupleData       TupleData message part representing the contents of the old tuple or primary key. Only present if the previous 'O' or 'K' part is present.
        Byte1('N')      Identifies the following TupleData message as a new tuple.
        TupleData       TupleData message part representing the contents of a new tuple.

                        INT16 number of attrs; for each attr:
                          1 Byte kind -- 'n'ull 'u'nchanged 't'ext 'b'inary
                          't/b' - INT32 length; then read data and null terminate

        The Update message may contain either a 'K' message part or an 'O' message part or
        neither of them, but never both of them.
    */

    int pos = 1;
    /*
    int32_t xid = recvint32(&buffer[pos]);     // only present in v2
    pos += 4;
    */

    int32_t rel_id = recvint32(&buffer[pos]);
    pos += 4;

    char old_type = buffer[pos];
    if (old_type == 'K' || old_type == 'O') {
        pos += 1;
    } else {
        // no type present
        old_type = '\0';
    }

    pos += decodeTuple(&buffer[pos], length - pos, msg.msg.update.old_tuple);

    char new_type = buffer[pos]; // should be 'N'
    if (new_type == 'N') {
        pos += 1;
    } else {
        // no type present
        // XXX check if this means no tuple to decode...
        new_type = '\0';
    }

    int r = decodeTuple(&buffer[pos], length - pos, msg.msg.update.new_tuple);
    if (r < 0) {
        return r;
    }
    pos += r;

    msg.msg_type = PgReplMsgType::UPDATE;
    msg.msg.update.rel_id = rel_id;
    msg.msg.update.old_type = old_type;
    msg.msg.update.new_type = new_type;

    return pos;
}


int PgReplMsg::decodeDelete(const char *buffer, int length,
                            PgReplMsgDecoded &msg)
{
    /*
        Byte1('D')      Identifies the message as a delete message.
        Int32           XID present since version 2 (PG14)
        Int32           ID of the relation corresponding to the ID in the relation message.
        Byte1('K')      Identifies the following TupleData submessage as a key.
                        This field is present if the table in which the delete has happened uses an index
                        as REPLICA IDENTITY.
        Byte1('O')      Identifies the following TupleData message as a old tuple.
                        This field is present if the table in which the delete has happened has
                        REPLICA IDENTITY set to FULL.
        TupleData       TupleData message part representing the contents of the old tuple or primary key,
                        depending on the previous field.

        The Delete message may contain either a 'K' message part or an 'O' message part,
        but never both of them.
    */

    int pos = 1;

    /*
    int32_t xid = recvint32(&buffer[pos]);     // only present in v2
    pos += 4;
    */

    int32_t rel_id = recvint32(&buffer[pos]);
    pos += 4;

    char type = buffer[pos];
    if (type == 'K' || type == 'O') {
        pos += 1;
    } else {
        // no type present
        type = '\0';
    }

    int r = decodeTuple(&buffer[pos], length -pos, msg.msg.delete_msg.tuple);
    if (r < 0) {
        return r;
    }
    pos += r;

    msg.msg_type = PgReplMsgType::DELETE;
    msg.msg.delete_msg.rel_id = rel_id;
    msg.msg.delete_msg.type = type;

    return pos;
}


int PgReplMsg::decodeTruncate(const char *buffer, int length,
                              PgReplMsgDecoded &msg)
{
    /*
        Byte1('T')      Identifies the message as a truncate message.
        Int32           XID present since version 2 (PG14)
        Int32           Number of relations
        Int8            Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
        Int32           ID of the relation corresponding to the ID in the relation message.
                        This field is repeated for each relation.
    */

    int pos = 1;

    /*
    int32_t xid = recvint32(&buffer[pos]);     // only present in v2
    pos += 4;
    */

    int32_t num_rels = recvint32(&buffer[pos]);
    pos += 4;

    int8_t options = (int8_t)buffer[pos];
    pos += 1;

    for (int i = 0; i < num_rels; i++) {
        int32_t rel_id = recvint32(&buffer[pos]);
        msg.msg.truncate.rel_ids.push_back(rel_id);
        pos += 4;
    }

    msg.msg_type = PgReplMsgType::TRUNCATE;
    msg.msg.truncate.num_rels = num_rels;
    msg.msg.truncate.options = options;

    return pos;
}


int PgReplMsg::decodeType(const char *buffer, int length,
                          PgReplMsgDecoded &msg)
{
    /*
        Byte1('Y') Identifies the message as a type message.
        Int32 (TransactionId) Xid of the transaction (only present for streamed transactions).
              This field is available since protocol version 2.
        Int32 (Oid) OID of the data type.
        String Namespace (empty string for pg_catalog).
        String Name of the data type.
    */

    int pos = 1;

    /*
    int32_t xid = recvint32(&buffer[pos]); // only version 2+
    pos += 4;
    */

    int32_t oid = recvint32(&buffer[pos]);
    pos += 4;

    const char *namespace_str = &buffer[pos];
    pos += std::strlen(&buffer[pos]);

    const char *data_type =  &buffer[pos];
    pos += std::strlen(&buffer[pos]);

    msg.msg_type = PgReplMsgType::TYPE;
    msg.msg.type.oid = oid;
    msg.msg.type.namespace_str = namespace_str;
    msg.msg.type.data_type_str = data_type;

    return pos;
}

