#include <cstdlib>
#include <sstream>
#include <fmt/core.h>

#include <psql_cdc/exception.hh>
#include <psql_cdc/pg_repl_msg.hh>

namespace springtail
{
    PgReplMsg::PgReplMsg(int proto_version) noexcept
      : _proto_version(proto_version) {}


    /**
     * @brief Initialize message to empty/invalid message
     */
    void PgReplMsg::setBuffer(const char *buffer, int length) noexcept
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
    bool PgReplMsg::hasNextMsg() noexcept
    {
        return (_buffer_length > 0);
    }


    /**
     * @brief Retrieve next message from internal buffer
     * @return reference to internal decoded message
     * @throws PgMessageTooSmallError
     * @throws PgUnexpectedDataError
     * @throws PgUnknownMessageError
     */
    const PgReplMsgDecoded &PgReplMsg::decodeNextMsg()
    {
        // first byte is opcode
        char msg_type = _buffer[0];
        int pos = 0;

        // initialize internal decoded message structure
        initMsg();

        switch(msg_type) {

            // V1 Protocol
            case MSG_BEGIN: // begin
                pos = decodeBegin();
                break;

            case MSG_COMMIT: // commit
                pos = decodeCommit();
                break;

            case MSG_RELATION: // relation
                pos = decodeRelation();
                break;

            case MSG_INSERT: // insert
                pos = decodeInsert();
                break;

            case MSG_UPDATE: // update
                pos = decodeUpdate();
                break;

            case MSG_DELETE: // delete
                pos = decodeDelete();
                break;

            case MSG_TRUNCATE: // truncate
                pos = decodeTruncate();
                break;

            case MSG_ORIGIN: // origin
                pos = decodeOrigin();
                break;

            case MSG_MESSAGE: // message
                pos = decodeMessage();
                break;

            case MSG_TYPE: // type
                pos = decodeType();
                break;

            case MSG_STREAM_START:
                pos = decodeStreamStart();
                break;

            case MSG_STREAM_STOP:
                pos = decodeStreamStop();
                break;

            case MSG_STREAM_COMMIT:
                pos = decodeStreamCommit();
                break;

            case MSG_STREAM_ABORT:
                pos = decodeStreamAbort();
                break;

            default: // unknown/unhandled
                std::cerr << "Unknown opcode to decode: " << msg_type << std::endl;
                setBuffer(nullptr, 0);
                throw PgUnknownMessageError();
        }

        // sanity check
        if (pos > _buffer_length) {
            std::cerr << "Buffer overrun in decode: consumed="
                      << pos << ", bytes available=" << _buffer_length << std::endl;

            setBuffer(nullptr, 0);
            /* Note: an error here will really require closing and re-opening the
             * replication stream to try and re-read the data */

            throw PgUnexpectedDataError();
        }

        _buffer_length -= pos;
        _buffer += pos;

        return _decoded_msg;
    }


    /**
     * @brief decode string from buffer given max length; check for null terminating char
     *
     * @param buffer input buffer
     * @param length length of input buffer
     * @param str_out output string (set to input_buffer on success, null on error)
     * @return length of string, -1 on error
     */
    int PgReplMsg::decodeString(const char *buffer, int length, const char **str_out)
    {
        if (length < 0) {
            std::cerr << "DecodeString: message has been consumed\n";
            throw PgMessageTooSmallError();
        }

        int len = strnlen(buffer, length);
        int null_offset = 0;

        if (len == length) {
            // probably not a valid string, as strings need to be null terminated
            // and strlen doesn't include the null char in the length
            null_offset = len;
        } else {
            null_offset = len + 1;
        }

        // sanity check
        if (buffer[null_offset] != '\0') {
            *str_out = nullptr;
            std::cerr << "DecodeString: error decoding string\n";
            throw PgUnexpectedDataError();
        }

        *str_out = buffer;
        return null_offset;
    }


    /**
     * @brief Decode tuple data within a message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decodeTuple(const char *buffer, int length, MsgTupleData &tuple)
    {
        /*
            TupleData
            Int16  Number of columns.
            Next, one of the following submessages appears for each column (except generated columns):
                Byte1('n') Identifies the data as NULL value. (no more data sent)
            Or
                Byte1('u') Identifies unchanged TOASTed value (the actual value is not sent).
            Or
                Byte1('t') Identifies the data as text formatted value.
                Int32 Length of the column value.
                Byten The value of the column, in text format. (A future release might support additional formats.) n is the above length.
         */
        if (length < 0) {
            std::cerr << "DecodeTuple: message has been consumed\n";
            throw PgMessageTooSmallError();
        }

        int pos = 0;

        tuple.num_columns = recvint16(&buffer[pos]);
        pos += 2;

        tuple.tuple_data.resize(tuple.num_columns);

        for (int i = 0; i < tuple.num_columns; i++) {
            if (pos >= length) {
                std::cerr << "DecodeTuple: error, consumed too much data\n";
                throw PgUnexpectedDataError();
            }

            tuple.tuple_data[i].type = buffer[pos];
            pos += 1;

            if (tuple.tuple_data[i].type == 'n' ||
                tuple.tuple_data[i].type == 'u') {
                tuple.tuple_data[i].data_len = 0;
                tuple.tuple_data[i].data = nullptr;
                continue;
            }

            tuple.tuple_data[i].data_len = recvint32(&buffer[pos]);
            pos += 4;

            tuple.tuple_data[i].data = &buffer[pos];
            pos += tuple.tuple_data[i].data_len;
        }

        return pos;
    }


    /**
     * @brief decode Message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decodeMessage()
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

        MsgMessage message;

        if (_proto_version > 1) {
            message.xid = recvint32(&_buffer[pos]);  // only version 2
            pos += 4;
        }

        message.flags = (int8_t)_buffer[pos];
        pos += 1;

        message.lsn = recvint64(&_buffer[pos]);
        pos += 8;

        int str_len = decodeString(&_buffer[pos], _buffer_length - pos, &message.prefix_str);
        pos += str_len;

        message.data_len = recvint32(&_buffer[pos]);
        pos += 4;

        message.data = &_buffer[pos];
        pos += message.data_len;

        _decoded_msg.msg_type = PgReplMsgType::MESSAGE;
        _decoded_msg.msg.emplace<MsgMessage>(message);

        return pos;
    }


    int PgReplMsg::decodeOrigin()
    {
        /*
            Byte1('O') Identifies the message as an origin message.
            Int64 (XLogRecPtr) The LSN of the commit on the origin server.
            String Name of the origin.

            Note that there can be multiple Origin messages inside a single transaction.
        */
        int pos = 1;

        MsgOrigin origin;

        origin.commit_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        int str_len = decodeString(&_buffer[pos], _buffer_length - pos, &origin.name_str);
        if (str_len == -1) {
            return -1;
        }
        pos += str_len;

        _decoded_msg.msg_type = PgReplMsgType::ORIGIN;
        _decoded_msg.msg.emplace<MsgOrigin>(origin);

        return pos;
    }


    int PgReplMsg::decodeBegin()
    {
        /*
            Byte1('B') Identifies the message as a begin message.
            Int64 The final LSN of the transaction.
            Int64 Commit timestamp of the transaction. Number of microseconds since Y2K
            Int32 Xid of the transaction.
        */
        int pos = 1;

        MsgBegin begin;

        begin.xact_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        begin.commit_ts = recvint64(&_buffer[pos]);
        pos += 8;

        begin.xid = recvint32(&_buffer[pos]);
        pos += 4;

        _decoded_msg.msg_type = PgReplMsgType::BEGIN;
        _decoded_msg.msg.emplace<MsgBegin>(begin);

        return pos;
    }


    int PgReplMsg::decodeCommit()
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

        commit.commit_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        commit.xact_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        commit.commit_ts = recvint64(&_buffer[pos]);
        pos += 8;

        _decoded_msg.msg_type = PgReplMsgType::COMMIT;
        _decoded_msg.msg.emplace<MsgCommit>(commit);

        return pos;
    }


    int PgReplMsg::decodeRelation()
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

        MsgRelation relation;

        if (_proto_version > 1) {
            relation.xid = recvint32(&_buffer[pos]);     // only present in v2
            pos += 4;
        }

        relation.rel_id = recvint32(&_buffer[pos]);
        pos += 4;

        int str_len = decodeString(&_buffer[pos], _buffer_length - pos, &relation.namespace_str);
        pos += str_len;

        str_len = decodeString(&_buffer[pos], _buffer_length - pos, &relation.rel_name_str);
        pos += str_len;

        relation.identity = (int8_t)_buffer[pos];
        pos += 1;

        relation.num_columns = recvint16(&_buffer[pos]);
        pos += 2;

        relation.columns.resize(relation.num_columns);

        // sanity check, need at least 10B per column
        if (_buffer_length - pos < (10 * relation.num_columns)) {
            throw PgMessageTooSmallError();
        }

        for (int i = 0; i < relation.num_columns; i++) {
            relation.columns[i].flags = *((int8_t *)&_buffer[pos]); // 0 no flags; 1 key
            pos += 1;

            str_len = decodeString(&_buffer[pos], _buffer_length - pos,
                                   &relation.columns[i].column_name);
            pos += str_len;

            relation.columns[i].oid = recvint32(&_buffer[pos]);
            pos += 4;

            relation.columns[i].type_modifier = recvint32(&_buffer[pos]);
            pos += 4;
        }

        _decoded_msg.msg_type = PgReplMsgType::RELATION;
        _decoded_msg.msg.emplace<MsgRelation>(relation);

        return pos;
    }


    int PgReplMsg::decodeInsert()
    {
        /*
            Byte1('I')  Identifies the message as an insert message.
            Int32 ID of the relation corresponding to the ID in the relation message
            Int32 XID present since version 2 (PG14)
            Byte1('N') Identifies the following TupleData message as a new tuple.
            TupleData TupleData message part representing the contents of new tuple.
        */
        int pos = 1;

        MsgInsert insert;

        if (_proto_version > 1) {
            insert.xid = recvint32(&_buffer[pos]);     // only present in v2
            pos += 4;
        }

        insert.rel_id = recvint32(&_buffer[pos]);
        pos += 4;

        insert.new_type = _buffer[pos]; // should be 'N'
        if (insert.new_type == 'N') {
            pos += 1;
        } else {
            // no type present
            // XXX check if this means no tuple to decode...
            insert.new_type = '\0';
            pos += 1;
        }

        pos += decodeTuple(&_buffer[pos], _buffer_length - pos, insert.new_tuple);

        _decoded_msg.msg_type = PgReplMsgType::INSERT;
        _decoded_msg.msg.emplace<MsgInsert>(insert);

        return pos;
    }


    int PgReplMsg::decodeUpdate()
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

        MsgUpdate update;

        if (_proto_version > 1) {
            update.xid = recvint32(&_buffer[pos]);     // only present in v2
            pos += 4;
        }

        update.rel_id = recvint32(&_buffer[pos]);
        pos += 4;

        update.old_type = _buffer[pos];
        if (update.old_type == 'K' || update.old_type == 'O') {
            pos += 1;
        } else {
            // no type present
            update.old_type = '\0';
        }

        pos += decodeTuple(&_buffer[pos], _buffer_length - pos, update.old_tuple);

        update.new_type = _buffer[pos]; // should be 'N'
        if (update.new_type == 'N') {
            pos += 1;
        } else {
            // no type present
            // XXX check if this means no tuple to decode...
            update.new_type = '\0';
        }

        pos += decodeTuple(&_buffer[pos], _buffer_length - pos, update.new_tuple);

        _decoded_msg.msg_type = PgReplMsgType::UPDATE;
        _decoded_msg.msg.emplace<MsgUpdate>(update);

        return pos;
    }


    int PgReplMsg::decodeDelete()
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

        MsgDelete delete_msg;

        if (_proto_version > 1) {
            delete_msg.xid = recvint32(&_buffer[pos]);     // only present in v2
            pos += 4;
        }

        delete_msg.rel_id = recvint32(&_buffer[pos]);
        pos += 4;

        delete_msg.type = _buffer[pos];
        if (delete_msg.type == 'K' || delete_msg.type == 'O') {
            pos += 1;
        } else {
            // no type present
            delete_msg.type = '\0';
        }

        pos += decodeTuple(&_buffer[pos], _buffer_length - pos, delete_msg.tuple);

        _decoded_msg.msg_type = PgReplMsgType::DELETE;
        _decoded_msg.msg.emplace<MsgDelete>(delete_msg);

        return pos;
    }


    int PgReplMsg::decodeTruncate()
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

        MsgTruncate truncate;

        if (_proto_version > 1) {
            truncate.xid = recvint32(&_buffer[pos]);     // only present in v2
            pos += 4;
        }

        truncate.num_rels = recvint32(&_buffer[pos]);
        pos += 4;

        truncate.options = (int8_t)_buffer[pos];
        pos += 1;

        // sanity check
        if (_buffer_length - pos < truncate.num_rels * 4) {
            throw PgMessageTooSmallError();
        }

        truncate.rel_ids.resize(truncate.num_rels);
        for (int i = 0; i < truncate.num_rels; i++) {
            truncate.rel_ids[i] = recvint32(&_buffer[pos]);
            pos += 4;
        }

        _decoded_msg.msg_type = PgReplMsgType::TRUNCATE;
        _decoded_msg.msg.emplace<MsgTruncate>(truncate);

        return pos;
    }


    int PgReplMsg::decodeType()
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

        MsgType type;

        if (_proto_version > 1) {
            type.xid = recvint32(&_buffer[pos]); // only version 2+
            pos += 4;
        }

        type.oid = recvint32(&_buffer[pos]);
        pos += 4;

        int str_len = decodeString(&_buffer[pos], _buffer_length - pos, &type.namespace_str);
        pos += str_len;

        str_len = decodeString(&_buffer[pos], _buffer_length - pos, &type.data_type_str);
        pos += str_len;

        _decoded_msg.msg_type = PgReplMsgType::TYPE;
        _decoded_msg.msg.emplace<MsgType>(type);

        return pos;
    }


    /**
     * @brief Stream start message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decodeStreamStart()
    {
        /*
            Byte1('S')  Identifies the message as a stream start message.
            Int32       Xid of the transaction.
            Int8_t      A value of 1 indicates this is the first stream segment for this XID, 0 for any other stream segment.
        */

        int pos = 1;

        MsgStreamStart stream_start;

        stream_start.xid = recvint32(&_buffer[pos]);
        pos += 4;

        stream_start.first = ((int8_t)_buffer[pos] == 1);
        pos += 1;

        _decoded_msg.msg_type = PgReplMsgType::STREAM_START;
        _decoded_msg.msg.emplace<MsgStreamStart>(stream_start);

        return pos;
    }


    /**
     * @brief Stream stop message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decodeStreamStop()
    {
        /*
            Byte1('E')  Identifies the message as a stream stop message.
        */

        int pos = 1;

        MsgStreamStop stream_stop;

        _decoded_msg.msg_type = PgReplMsgType::STREAM_STOP;
        _decoded_msg.msg.emplace<MsgStreamStop>(stream_stop);

        return pos;
    }


    /**
     * @brief Stream commit message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decodeStreamCommit()
    {
        /*
            Byte1('c')  Identifies the message as a stream commit message.
            Int32       Xid of the transaction.
            Int8(0)     Flags; currently unused.
            Int64       The LSN of the commit.
            Int64       The end LSN of the transaction.
            Int64       Commit timestamp of the transaction. The value is in number of
                        microseconds since PostgreSQL epoch (2000-01-01).
        */

        int pos = 1;

        MsgStreamCommit stream_commit;

        stream_commit.xid = recvint32(&_buffer[pos]);
        pos += 4;

        // skip flags
        pos += 1;

        stream_commit.commit_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        stream_commit.xact_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        stream_commit.commit_ts = recvint64(&_buffer[pos]);
        pos += 8;

        _decoded_msg.msg_type = PgReplMsgType::STREAM_COMMIT;
        _decoded_msg.msg.emplace<MsgStreamCommit>(stream_commit);

        return pos;
    }


    /**
     * @brief Stream abort message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decodeStreamAbort()
    {
        /*
            Byte1('A')  Identifies the message as a stream abort message.
            Int32       Xid of the transaction.
            Int32       Xid of the subtransaction (will be same as xid of the transaction for top-level transactions).
            Int64       The LSN of the abort. This field is available since protocol version 4.
            Int64       Abort timestamp of the transaction. The value is in number of
                        microseconds since PostgreSQL epoch (2000-01-01). This field is available
                        since protocol version 4.
        */
        int pos = 1;

        MsgStreamAbort stream_abort;

        stream_abort.xid = recvint32(&_buffer[pos]);
        pos += 4;

        stream_abort.sub_xid = recvint32(&_buffer[pos]);
        pos += 4;

        if (_proto_version > 3) {
            stream_abort.abort_lsn = recvint64(&_buffer[pos]);
            pos += 8;

            stream_abort.abort_ts = recvint64(&_buffer[pos]);
            pos += 8;
        }

        _decoded_msg.msg_type = PgReplMsgType::STREAM_ABORT;
        _decoded_msg.msg.emplace<MsgStreamAbort>(stream_abort);

        return pos;
    }


    void PgReplMsg::dumpTuple(const MsgTupleData &tuple,
                              std::stringstream &ss) noexcept
    {
        for (int i = 0; i < tuple.num_columns; i++) {
            ss << "  - type=" << tuple.tuple_data[i].type << std::endl;
            ss << "  - data_len=" << tuple.tuple_data[i].data_len << std::endl;
        }
    }

    /**
     * @brief Convert LSN to string of format XXX/XXX
     *
     * @param lsn LSN to convert
     * @return string of LSN in format: "XXX/XXX"
     */
    std::string PgReplMsg::lsnToStr(const LSN_t lsn) noexcept
    {
        uint32_t lsn_higher = (uint32_t)(lsn>>32);
        uint32_t lsn_lower = (uint32_t)(lsn);

        return fmt::format("{:X}/{:X}", lsn_higher, lsn_lower);
    }

    /**
     * @brief Convert LSN in string format XXX/XXX to LSN_t (uint64_t)
     *
     * @param lsn_str string of LSN in format XXX/XXX
     * @return LSN_t
     */
    LSN_t PgReplMsg::strToLSN(const char *lsn_str) noexcept
    {
        char *end_ptr = nullptr;

        if (lsn_str == nullptr) {
            return INVALID_LSN;
        }

        // convert high bits
        uint64_t lsn_higher = strtol(lsn_str, &end_ptr, 16);

        // end_ptr now points to the '/' -- validate
        if (end_ptr == nullptr || *end_ptr != '/') {
            return INVALID_LSN;
        }

        // convert low bits starting at end_ptr + 1
        uint64_t lsn_lower = strtol(end_ptr+1, nullptr, 16);

        return (lsn_higher << 32) | (0xFFFFFFFF & lsn_lower);
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
                    ss << "  - key=" << (relation.columns[i].flags == 1) << std::endl;
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

            case STREAM_START: {
                MsgStreamStart start = std::get<MsgStreamStart>(msg.msg);
                ss << "\nSTREAM START" << std::endl;
                ss << "  xid=" << start.xid << std::endl;
                ss << "  first=" << start.first << std::endl;
                break;
            }

            case STREAM_STOP: {
                ss << "\nSTREAM STOP" << std::endl;
                break;
            }

            case STREAM_COMMIT: {
                MsgStreamCommit commit = std::get<MsgStreamCommit>(msg.msg);
                ss << "\nSTREAM COMMIT" << std::endl;
                ss << "  xid=" << commit.xid << std::endl;
                ss << "  commit LSN=" << commit.commit_lsn
                   << " (" << lsnToStr(commit.commit_lsn) << ")\n";
                ss << "  xact LSN=" << commit.xact_lsn
                   << " (" << lsnToStr(commit.xact_lsn) << ")\n";
                break;
            }

            case STREAM_ABORT: {
                MsgStreamAbort abort = std::get<MsgStreamAbort>(msg.msg);
                ss << "\nSTREAM ABORT" << std::endl;
                ss << "  xid=" << abort.xid << std::endl;
                ss << "  sub_xid=" << abort.sub_xid << std::endl;
                break;
            }

            default:
                break;
        }

        return ss.str();
    }

}