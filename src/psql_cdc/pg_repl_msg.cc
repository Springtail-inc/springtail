#include <sstream>
#include <fmt/core.h>

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

    std::cout << "Message type: " << msg_type << std::endl;

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
        std::cerr << "Buffer overrun in decode: consumed="
                  << pos << ", bytes available=" << _buffer_length << std::endl;

        initMsg();
        _buffer = nullptr;
        _buffer_length = 0;

        return _decoded_msg;
    }

    std::cout << "Message decode consumed: " << pos << "/" << _buffer_length << std::endl;

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

    tuple.num_columns = recvint16(&buffer[pos]);
    pos += 2;

    tuple.tuple_data.resize(tuple.num_columns);

    for (int i = 0; i < tuple.num_columns; i++) {
        char type = buffer[pos];
        pos += 1;

        int32_t data_len = recvint32(&buffer[pos]);
        pos += 4;

        const char *data = &buffer[pos];
        pos += data_len;

        tuple.tuple_data[i].type = type;
        tuple.tuple_data[i].data_len = data_len;
        tuple.tuple_data[i].data = data;
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

    MsgMessage message;

    message.flags = (int8_t)buffer[pos];
    pos += 1;

    message.lsn = recvint64(&buffer[pos]);
    pos += 8;

    message.prefix_str = &buffer[pos];
    pos += strnlen(&buffer[pos], length-pos) + 1;

    message.data_len = recvint32(&buffer[pos]);
    pos += 4;

    message.data = &buffer[pos];
    pos += message.data_len;

    msg.msg_type = PgReplMsgType::MESSAGE;
    msg.msg.emplace<MsgMessage>(message);

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

    MsgOrigin origin;

    origin.commit_lsn = recvint64(&buffer[pos]);
    pos += 8;

    origin.name_str = &buffer[pos];
    pos += strnlen(&buffer[pos], length - pos) + 1;

    msg.msg_type = PgReplMsgType::ORIGIN;
    msg.msg.emplace<MsgOrigin>(origin);

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

    MsgBegin begin;

    begin.xact_lsn = recvint64(&buffer[pos]);
    pos += 8;

    begin.commit_ts = recvint64(&buffer[pos]);
    pos += 8;

    begin.xid = recvint32(&buffer[pos]);
    pos += 4;

    msg.msg_type = PgReplMsgType::BEGIN;
    msg.msg.emplace<MsgBegin>(begin);

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

    MsgCommit commit;

    commit.commit_lsn = recvint64(&buffer[pos]);
    pos += 8;

    commit.xact_lsn = recvint64(&buffer[pos]);
    pos += 8;

    commit.commit_ts = recvint64(&buffer[pos]);
    pos += 8;

    msg.msg_type = PgReplMsgType::COMMIT;
    msg.msg.emplace<MsgCommit>(commit);

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

    MsgRelation relation;

    relation.rel_id = recvint32(&buffer[pos]);
    pos += 4;

    relation.namespace_str = &buffer[pos];
    pos += strnlen(&buffer[pos], length - pos) + 1;

    relation.rel_name_str = &buffer[pos];
    pos += strnlen(&buffer[pos], length - pos) + 1;

    relation.identity = (int8_t)buffer[pos];
    pos += 1;

    relation.num_columns = recvint16(&buffer[pos]);
    pos += 2;

    relation.columns.resize(relation.num_columns);

    for (int i = 0; i < relation.num_columns; i++) {
        relation.columns[i].flags = (int8_t)buffer[pos]; // 0 no flags; 1 key
        pos += 1;

        relation.columns[i].column_name = &buffer[pos];
        pos += strnlen(&buffer[pos], length - pos) + 1;

        relation.columns[i].oid = recvint32(&buffer[pos]);
        pos += 4;

        relation.columns[i].type_modifier = recvint32(&buffer[pos]);
        pos += 4;
    }

    msg.msg_type = PgReplMsgType::RELATION;
    msg.msg.emplace<MsgRelation>(relation);

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

    MsgInsert insert;

    insert.rel_id = recvint32(&buffer[pos]);
    pos += 4;

    insert.new_type = buffer[pos]; // should be 'N'
    if (insert.new_type == 'N') {
        pos += 1;
    } else {
        // no type present
        // XXX check if this means no tuple to decode...
        insert.new_type = '\0';
        pos += 1;
    }

    pos += decodeTuple(&buffer[pos], length - pos, insert.new_tuple);

    msg.msg_type = PgReplMsgType::INSERT;
    msg.msg.emplace<MsgInsert>(insert);

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

    MsgUpdate update;

    update.rel_id = recvint32(&buffer[pos]);
    pos += 4;

    update.old_type = buffer[pos];
    if (update.old_type == 'K' || update.old_type == 'O') {
        pos += 1;
    } else {
        // no type present
        update.old_type = '\0';
    }

    pos += decodeTuple(&buffer[pos], length - pos, update.old_tuple);

    update.new_type = buffer[pos]; // should be 'N'
    if (update.new_type == 'N') {
        pos += 1;
    } else {
        // no type present
        // XXX check if this means no tuple to decode...
        update.new_type = '\0';
    }

    pos += decodeTuple(&buffer[pos], length - pos, update.new_tuple);

    msg.msg_type = PgReplMsgType::UPDATE;
    msg.msg.emplace<MsgUpdate>(update);

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

    MsgDelete delete_msg;

    delete_msg.rel_id = recvint32(&buffer[pos]);
    pos += 4;

    delete_msg.type = buffer[pos];
    if (delete_msg.type == 'K' || delete_msg.type == 'O') {
        pos += 1;
    } else {
        // no type present
        delete_msg.type = '\0';
    }

    pos += decodeTuple(&buffer[pos], length - pos, delete_msg.tuple);

    msg.msg_type = PgReplMsgType::DELETE;
    msg.msg.emplace<MsgDelete>(delete_msg);

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

    MsgTruncate truncate;

    truncate.num_rels = recvint32(&buffer[pos]);
    pos += 4;

    truncate.options = (int8_t)buffer[pos];
    pos += 1;

    truncate.rel_ids.resize(truncate.num_rels);
    for (int i = 0; i < truncate.num_rels; i++) {
        truncate.rel_ids[i] = recvint32(&buffer[pos]);
        pos += 4;
    }

    msg.msg_type = PgReplMsgType::TRUNCATE;
    msg.msg.emplace<MsgTruncate>(truncate);

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

    MsgType type;

    type.oid = recvint32(&buffer[pos]);
    pos += 4;

    type.namespace_str = &buffer[pos];
    pos += strnlen(&buffer[pos], length - pos) + 1;

    type.data_type_str =  &buffer[pos];
    pos += strnlen(&buffer[pos], length - pos) + 1;

    msg.msg_type = PgReplMsgType::TYPE;
    msg.msg.emplace<MsgType>(type);

    return pos;
}

void PgReplMsg::dumpTuple(const MsgTupleData &tuple,
                          std::stringstream &ss)
{
    for (int i = 0; i < tuple.num_columns; i++) {
        ss << "  - type=" << tuple.tuple_data[i].type << std::endl;
        ss << "  - data_len=" << tuple.tuple_data[i].data_len << std::endl;
    }
}

std::string PgReplMsg::lsnToStr(const LSN_t lsn)
{
    uint32_t lsn_higher = (uint32_t)(lsn>>32);
    uint32_t lsn_lower = (uint32_t)(lsn);

    return fmt::format("{:X}/{:X}", lsn_higher, lsn_lower);
}

/**
 * @brief convert a message to a printable string
 *
 * @param msg refernece to message to convert
 * @return readable string of msg
 */
std::string PgReplMsg::dumpMsg(const PgReplMsgDecoded &msg)
{
    std::stringstream ss;

    switch(msg.msg_type) {
        case BEGIN: {
            MsgBegin begin = std::get<MsgBegin>(msg.msg);
            ss << "\nBEGIN" << std::endl;
            ss << "  xid=" << begin.xid << std::endl;
            ss << "  LSN=" << begin.xact_lsn << " ("
               << lsnToStr(begin.xact_lsn) << ")\n";
            break;
        }

        case COMMIT: {
            MsgCommit commit = std::get<MsgCommit>(msg.msg);
            ss << "\nCOMMIT" << std::endl;
            ss << "  commit LSN=" << commit.commit_lsn
               << " (" << lsnToStr(commit.commit_lsn) << ")\n";
            ss << "  xact LSN=" << commit.xact_lsn
               << " (" << lsnToStr(commit.xact_lsn) << ")\n";
            break;
        }

        case RELATION: {
            MsgRelation relation = std::get<MsgRelation>(msg.msg);
            ss << "\nRELATION" << std::endl;
            ss << "  rel_id=" << relation.rel_id << std::endl;
            ss << "  namespace=" << relation.namespace_str << std::endl;
            ss << "  rel_name=" << relation.rel_name_str << std::endl;

            ss << "  Columns" << std::endl;
            for (int i = 0; i < relation.num_columns; i++) {
                ss << "  - name=" << relation.columns[i].column_name << std::endl;
                ss << "  - key=" << relation.columns[i].flags << std::endl;
                ss << "  - oid=" << relation.columns[i].oid << std::endl;
                ss << "  - type modifier=" << relation.columns[i].type_modifier << std::endl;
            }

            break;
        }

        case INSERT: {
            MsgInsert insert = std::get<MsgInsert>(msg.msg);
            ss << "\nINSERT" << std::endl;
            ss << "  rel_id=" << insert.rel_id << std::endl;
            ss << "  New tuples" << std::endl;
            dumpTuple(insert.new_tuple, ss);
            break;
        }

        case DELETE: {
            MsgDelete delete_msg = std::get<MsgDelete>(msg.msg);
            ss << "\nDELETE";
            ss << "  rel_id=" << delete_msg.rel_id << std::endl;
            ss << "  Tuples\n";
            dumpTuple(delete_msg.tuple, ss);
            break;
        }

        case UPDATE: {
            MsgUpdate update = std::get<MsgUpdate>(msg.msg);
            ss << "\nUPDATE";
            ss << "  rel_id=" << update.rel_id << std::endl;
            ss << "  Old tuples" << std::endl;
            dumpTuple(update.old_tuple, ss);
            ss << "  New tuples" << std::endl;
            dumpTuple(update.new_tuple, ss);
            break;
        }

        case TRUNCATE: {
            MsgTruncate truncate = std::get<MsgTruncate>(msg.msg);
            ss << "\nTRUNCATE" << std::endl;
            for (int32_t rel_id: truncate.rel_ids) {
                ss << "  rel_id=" << rel_id << std::endl;
            }
            break;
        }

        case ORIGIN: {
            MsgOrigin origin = std::get<MsgOrigin>(msg.msg);
            ss << "\nORIGIN" << std::endl;
            ss << "  commit LSN=" << origin.commit_lsn
               << " (" << lsnToStr(origin.commit_lsn) << ")\n";
            ss << "  name=" << origin.name_str << std::endl;
            break;
        }

        case MESSAGE: {
            MsgMessage message = std::get<MsgMessage>(msg.msg);
            ss << "\nMESSAGE" << std::endl;
            ss << "  xid=" << message.xid << std::endl;
            ss << "  LSN=" << message.lsn
               << " (" << lsnToStr(message.lsn) << ")\n";
            ss << "  prefix=" << message.prefix_str << std::endl;
            break;
        }

        case TYPE: {
            MsgType type = std::get<MsgType>(msg.msg);
            ss << "\nTYPE" << std::endl;
            ss << "  xid=" << type.xid << std::endl;
            ss << "  oid=" << type.oid << std::endl;
            ss << "  namespace=" << type.namespace_str << std::endl;
            ss << "  data type=" << type.data_type_str << std::endl;
            break;
        }

        default:
            break;
    }

    return ss.str();
}

