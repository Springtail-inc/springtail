#include <cstdlib>
#include <sstream>
#include <variant>
#include <algorithm>

#include <fmt/core.h>
#include <nlohmann/json.hpp>

#include <common/logging.hh>
#include <common/common.hh>

#include <pg_repl/exception.hh>
#include <pg_repl/pg_repl_msg.hh>

namespace springtail
{
    /**
     * @brief Initialize message to empty/invalid message
     */
    void PgReplMsg::set_buffer(const char *buffer, int length) noexcept
    {
        _buffer = buffer;
        _buffer_length = length;
        init_msg();
    }


    /**
     * @brief Does more data exist to process
     *
     * @return true if more data exists, false otherwise
     */
    bool PgReplMsg::has_next_msg() noexcept
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
    const PgReplMsgDecoded &PgReplMsg::decode_next_msg()
    {
        // first byte is opcode
        char msg_type = _buffer[0];
        int pos = 0;

        // initialize internal decoded message structure
        init_msg();

        switch(msg_type) {

            // V1 Protocol
            case MSG_BEGIN: // begin
                pos = decode_begin();
                break;

            case MSG_COMMIT: // commit
                pos = decode_commit();
                break;

            case MSG_RELATION: // relation
                pos = decode_relation();
                break;

            case MSG_INSERT: // insert
                pos = decode_insert();
                break;

            case MSG_UPDATE: // update
                pos = decode_update();
                break;

            case MSG_DELETE: // delete
                pos = decode_delete();
                break;

            case MSG_TRUNCATE: // truncate
                pos = decode_truncate();
                break;

            case MSG_ORIGIN: // origin
                pos = decode_origin();
                break;

            case MSG_MESSAGE: // message
                pos = decode_message();
                break;

            case MSG_TYPE: // type
                pos = decode_type();
                break;

            case MSG_STREAM_START:
                pos = decode_stream_start();
                break;

            case MSG_STREAM_STOP:
                pos = decode_stream_stop();
                break;

            case MSG_STREAM_COMMIT:
                pos = decode_stream_commit();
                break;

            case MSG_STREAM_ABORT:
                pos = decode_stream_abort();
                break;

            default: // unknown/unhandled
                std::cerr << "Unknown opcode to decode: " << msg_type << std::endl;
                set_buffer(nullptr, 0);
                throw PgUnknownMessageError();
        }

        // sanity check
        if (pos > _buffer_length) {
            std::cerr << "Buffer overrun in decode: consumed="
                      << pos << ", bytes available=" << _buffer_length << std::endl;

            set_buffer(nullptr, 0);
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
    int PgReplMsg::decode_string(const char *buffer, int length, const char **str_out)
    {
        if (length < 0) {
            std::cerr << "decode_string: message has been consumed\n";
            throw PgMessageTooSmallError();
        }

        int len = strnlen(buffer, length);
        int null_offset = 0;

        if (len == length) {
            // probably not a valid string, as strings need to be null terminated
            // and strlen doesn't include the null char in the length
            null_offset = len - 1;
        } else {
            null_offset = len;
        }

        // sanity check
        if (buffer[null_offset] != '\0') {
            *str_out = nullptr;
            std::cerr << "decode_string: error decoding string\n";
            throw PgUnexpectedDataError();
        }

        *str_out = buffer;
        return null_offset + 1;
    }


    /**
     * @brief Decode tuple data within a message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decode_tuple(const char *buffer, int length, PgMsgTupleData &tuple)
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
            std::cerr << "decode_tuple: message has been consumed\n";
            throw PgMessageTooSmallError();
        }

        int pos = 0;

        tuple.num_columns = recvint16(&buffer[pos]);
        pos += 2;

        tuple.tuple_data.resize(tuple.num_columns);

        for (int i = 0; i < tuple.num_columns; i++) {
            if (pos >= length) {
                std::cerr << "decode_tuple: error, consumed too much data\n";
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
     * @brief Decode columns found in create/alter table messages
     *
     * @param column_json JSON array of column schema data
     * @param columns Output vector containing struct MsgSchemaColumn
     */
    void PgReplMsg::decode_schema_columns(nlohmann::json &column_json,
                                          std::vector<PgMsgSchemaColumn> &columns)
    {
        // iterate through json array
        for (auto &el: column_json.items()) {
            PgMsgSchemaColumn column;
            nlohmann::json json = el.value();

            json["name"].get_to(column.column_name);
            json["type"].get_to(column.udt_type);
            json["is_nullable"].get_to(column.is_nullable);
            json["is_pkey"].get_to(column.is_pkey);
            json["position"].get_to(column.position);

            if (!json["default"].is_null()) {
                column.default_value = json["default"].get<std::string>();
            }

            columns.push_back(column);
        }
    }


    /**
     * @brief Decode create table message.  Stored as JSON in message data.
     *
     * @param msg Message containing JSON data
     * @return true if successfully handled
     */
    bool PgReplMsg::decode_create_table(PgMsgMessage &msg)
    {
        PgMsgTable table_msg;

        // convert msg data to string (it is not null terminated)
        // and convert string to json
        std::string data_str(msg.data, msg.data_len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        // check object type, could be an index, default value or something other
        // than a table
        std::string object_type;
        json["obj"].get_to(object_type);
        if (object_type != "table") {
            std::cout << "Create/alter table msg not for table object, for: "
                      << object_type << std::endl;
            return false;
        }

        table_msg.xid = msg.xid; // only valid in streaming mode
        table_msg.lsn = msg.lsn;
        json["schema"].get_to(table_msg.schema);
        json["oid"].get_to(table_msg.oid);

        // identity in form: schema.table; parse out table
        std::string identity;
        json["identity"].get_to(identity);
        auto const pos = identity.find_last_of('.');
        table_msg.table = identity.substr(pos + 1);

        decode_schema_columns(json["columns"], table_msg.columns);

        _decoded_msg.msg_type = PgReplMsgType::CREATE_TABLE;
        _decoded_msg.msg.emplace<PgMsgTable>(table_msg);

        return true;
    }


    /**
     * @brief Decode alter table message, it follows same format
     *        as create table message, but we store the message type
     *        differently so we can recoginize it as such.
     *
     * @param msg Message containing JSON data
     * @return true if successfully handled
     */
    bool PgReplMsg::decode_alter_table(PgMsgMessage &msg)
    {
        // same data as in create table, call that to do the decode and
        // then just switch the type so we know it is an alter table
        if (!decode_create_table(msg)) {
            return false;
        }

        _decoded_msg.msg_type = PgReplMsgType::ALTER_TABLE;

        return true;
    }


    /**
     * @brief Decode drop table message.
     *
     * @param msg Message containing JSON data
     * @return true if successfully handled
     */
    bool PgReplMsg::decode_drop_table(PgMsgMessage &msg)
    {
        PgMsgDropTable drop_table_msg;
        std::string data_str(msg.data, msg.data_len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        // check object type, could be an index, default value or something other
        // than a table; if so we skip decoding
        std::string object_type;
        json["obj"].get_to(object_type);
        if (object_type != "table") {
            std::cout << "Drop table not for table object, for: " << object_type << std::endl;
            return false;
        }

        drop_table_msg.xid = msg.xid; // only valid in streaming mode
        drop_table_msg.lsn = msg.lsn;

        json["oid"].get_to(drop_table_msg.oid);
        json["schema"].get_to(drop_table_msg.schema);
        json["name"].get_to(drop_table_msg.table);

        _decoded_msg.msg_type = PgReplMsgType::DROP_TABLE;
        _decoded_msg.msg.emplace<PgMsgDropTable>(drop_table_msg);

        return true;
    }


    /**
     * @brief decode Message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decode_message()
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

        PgMsgMessage message;

        if (_streaming) {
            message.xid = recvint32(&_buffer[pos]);  // only version 2
            pos += 4;
        } else {
            message.xid = 0;
        }

        message.flags = (int8_t)_buffer[pos];
        pos += 1;

        message.lsn = recvint64(&_buffer[pos]);
        pos += 8;

        pos += decode_string(&_buffer[pos], _buffer_length - pos, &message.prefix_str);

        message.data_len = recvint32(&_buffer[pos]);
        pos += 4;

        message.data = &_buffer[pos];
        pos += message.data_len;

        bool message_handled = false;
        if (strcmp(message.prefix_str, MSG_PREFIX_CREATE_TABLE) == 0) {
            message_handled = decode_create_table(message);
        } else if (strcmp(message.prefix_str, MSG_PREFIX_ALTER_TABLE) == 0) {
            message_handled = decode_alter_table(message);
        } else if (strcmp(message.prefix_str, MSG_PREFIX_DROP_TABLE) == 0) {
            message_handled = decode_drop_table(message);
        }

        if (!message_handled) {
            _decoded_msg.msg_type = PgReplMsgType::MESSAGE;
            _decoded_msg.msg.emplace<PgMsgMessage>(message);
        }

        return pos;
    }


    int PgReplMsg::decode_origin()
    {
        /*
            Byte1('O') Identifies the message as an origin message.
            Int64 (XLogRecPtr) The LSN of the commit on the origin server.
            String Name of the origin.

            Note that there can be multiple Origin messages inside a single transaction.
        */
        int pos = 1;

        PgMsgOrigin origin;

        origin.commit_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        pos += decode_string(&_buffer[pos], _buffer_length - pos, &origin.name_str);

        _decoded_msg.msg_type = PgReplMsgType::ORIGIN;
        _decoded_msg.msg.emplace<PgMsgOrigin>(origin);

        return pos;
    }


    int PgReplMsg::decode_begin()
    {
        /*
            Byte1('B') Identifies the message as a begin message.
            Int64 The final LSN of the transaction.
            Int64 Commit timestamp of the transaction. Number of microseconds since Y2K
            Int32 Xid of the transaction.
        */
        int pos = 1;

        PgMsgBegin begin;

        begin.xact_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        begin.commit_ts = recvint64(&_buffer[pos]);
        pos += 8;

        begin.xid = recvint32(&_buffer[pos]);
        pos += 4;

        _decoded_msg.msg_type = PgReplMsgType::BEGIN;
        _decoded_msg.msg.emplace<PgMsgBegin>(begin);

        return pos;
    }


    int PgReplMsg::decode_commit()
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

        PgMsgCommit commit;

        commit.commit_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        commit.xact_lsn = recvint64(&_buffer[pos]);
        pos += 8;

        commit.commit_ts = recvint64(&_buffer[pos]);
        pos += 8;

        _decoded_msg.msg_type = PgReplMsgType::COMMIT;
        _decoded_msg.msg.emplace<PgMsgCommit>(commit);

        return pos;
    }


    int PgReplMsg::decode_relation()
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

        PgMsgRelation relation;

        if (_streaming) {
            relation.xid = recvint32(&_buffer[pos]);     // only present in v2
            pos += 4;
        }

        relation.rel_id = recvint32(&_buffer[pos]);
        pos += 4;

        pos += decode_string(&_buffer[pos], _buffer_length - pos, &relation.namespace_str);

        pos += decode_string(&_buffer[pos], _buffer_length - pos, &relation.rel_name_str);

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

            pos += decode_string(&_buffer[pos], _buffer_length - pos,
                                &relation.columns[i].column_name);

            relation.columns[i].oid = recvint32(&_buffer[pos]);
            pos += 4;

            relation.columns[i].type_modifier = recvint32(&_buffer[pos]);
            pos += 4;
        }

        _decoded_msg.msg_type = PgReplMsgType::RELATION;
        _decoded_msg.msg.emplace<PgMsgRelation>(relation);

        return pos;
    }


    int PgReplMsg::decode_insert()
    {
        /*
            Byte1('I')  Identifies the message as an insert message.
            Int32 ID of the relation corresponding to the ID in the relation message
            Int32 XID present since version 2 (PG14)
            Byte1('N') Identifies the following TupleData message as a new tuple.
            TupleData TupleData message part representing the contents of new tuple.
        */
        int pos = 1;

        PgMsgInsert insert;

        if (_streaming) {
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

        pos += decode_tuple(&_buffer[pos], _buffer_length - pos, insert.new_tuple);

        _decoded_msg.msg_type = PgReplMsgType::INSERT;
        _decoded_msg.msg.emplace<PgMsgInsert>(insert);

        return pos;
    }


    int PgReplMsg::decode_update()
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

        PgMsgUpdate update;

        if (_streaming) {
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

        pos += decode_tuple(&_buffer[pos], _buffer_length - pos, update.old_tuple);

        update.new_type = _buffer[pos]; // should be 'N'
        if (update.new_type == 'N') {
            pos += 1;
        } else {
            // no type present
            // XXX check if this means no tuple to decode...
            update.new_type = '\0';
        }

        pos += decode_tuple(&_buffer[pos], _buffer_length - pos, update.new_tuple);

        _decoded_msg.msg_type = PgReplMsgType::UPDATE;
        _decoded_msg.msg.emplace<PgMsgUpdate>(update);

        return pos;
    }


    int PgReplMsg::decode_delete()
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

        PgMsgDelete delete_msg;

        if (_streaming) {
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

        pos += decode_tuple(&_buffer[pos], _buffer_length - pos, delete_msg.tuple);

        _decoded_msg.msg_type = PgReplMsgType::DELETE;
        _decoded_msg.msg.emplace<PgMsgDelete>(delete_msg);

        return pos;
    }


    int PgReplMsg::decode_truncate()
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

        PgMsgTruncate truncate;

        if (_streaming) {
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
        _decoded_msg.msg.emplace<PgMsgTruncate>(truncate);

        return pos;
    }


    int PgReplMsg::decode_type()
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

        PgMsgType type;

        if (_streaming) {
            type.xid = recvint32(&_buffer[pos]); // only version 2+
            pos += 4;
        }

        type.oid = recvint32(&_buffer[pos]);
        pos += 4;

        pos += decode_string(&_buffer[pos], _buffer_length - pos, &type.namespace_str);

        pos += decode_string(&_buffer[pos], _buffer_length - pos, &type.data_type_str);

        _decoded_msg.msg_type = PgReplMsgType::TYPE;
        _decoded_msg.msg.emplace<PgMsgType>(type);

        return pos;
    }


    /**
     * @brief Stream start message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decode_stream_start()
    {
        /*
            Byte1('S')  Identifies the message as a stream start message.
            Int32       Xid of the transaction.
            Int8_t      A value of 1 indicates this is the first stream segment for this XID, 0 for any other stream segment.
        */

        int pos = 1;

        PgMsgStreamStart stream_start;

        stream_start.xid = recvint32(&_buffer[pos]);
        pos += 4;

        stream_start.first = ((int8_t)_buffer[pos] == 1);
        pos += 1;

        _decoded_msg.msg_type = PgReplMsgType::STREAM_START;
        _decoded_msg.msg.emplace<PgMsgStreamStart>(stream_start);

        _streaming = true;

        return pos;
    }


    /**
     * @brief Stream stop message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decode_stream_stop()
    {
        /*
            Byte1('E')  Identifies the message as a stream stop message.
        */

        int pos = 1;

        PgMsgStreamStop stream_stop;

        _decoded_msg.msg_type = PgReplMsgType::STREAM_STOP;
        _decoded_msg.msg.emplace<PgMsgStreamStop>(stream_stop);

        _streaming = false;

        return pos;
    }


    /**
     * @brief Stream commit message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decode_stream_commit()
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

        PgMsgStreamCommit stream_commit;

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
        _decoded_msg.msg.emplace<PgMsgStreamCommit>(stream_commit);

        return pos;
    }


    /**
     * @brief Stream abort message
     *
     * @return number of bytes consumed
     */
    int PgReplMsg::decode_stream_abort()
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

        PgMsgStreamAbort stream_abort;

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
        _decoded_msg.msg.emplace<PgMsgStreamAbort>(stream_abort);

        return pos;
    }


    void PgReplMsg::dump_tuple(const PgMsgTupleData &tuple,
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
    std::string PgReplMsg::lsn_to_str(const LSN_t lsn) noexcept
    {
        uint32_t lsn_higher = static_cast<uint32_t>(lsn>>32);
        uint32_t lsn_lower = static_cast<uint32_t>(lsn);

        return fmt::format("{:X}/{:X}", lsn_higher, lsn_lower);
    }

    /**
     * @brief Convert LSN in string format XXX/XXX to LSN_t (uint64_t)
     *
     * @param lsn_str string of LSN in format XXX/XXX
     * @return LSN_t
     */
    LSN_t PgReplMsg::str_to_LSN(const char *lsn_str) noexcept
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
    std::string PgReplMsg::dump_msg(const PgReplMsgDecoded &msg)
    {
        std::stringstream ss;

        switch(msg.msg_type) {
            case BEGIN: {
                PgMsgBegin begin = std::get<PgMsgBegin>(msg.msg);
                ss << "\nBEGIN" << std::endl;
                ss << "  xid=" << begin.xid << std::endl;
                ss << "  LSN=" << begin.xact_lsn << " ("
                   << lsn_to_str(begin.xact_lsn) << ")\n";
                break;
            }

            case COMMIT: {
                PgMsgCommit commit = std::get<PgMsgCommit>(msg.msg);
                ss << "\nCOMMIT" << std::endl;
                ss << "  commit LSN=" << commit.commit_lsn
                   << " (" << lsn_to_str(commit.commit_lsn) << ")\n";
                ss << "  xact LSN=" << commit.xact_lsn
                   << " (" << lsn_to_str(commit.xact_lsn) << ")\n";
                break;
            }

            case RELATION: {
                PgMsgRelation relation = std::get<PgMsgRelation>(msg.msg);
                ss << "\nRELATION" << std::endl;
                if (_streaming) {
                    ss << "  xid=" << relation.xid << std::endl;
                }
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
                PgMsgInsert insert = std::get<PgMsgInsert>(msg.msg);
                ss << "\nINSERT" << " (" << msg.proto_version << ")" << std::endl;
                if (_streaming) {
                    ss << "  xid=" << insert.xid << std::endl;
                }
                ss << "  rel_id=" << insert.rel_id << std::endl;
                ss << "  New tuples" << std::endl;
                dump_tuple(insert.new_tuple, ss);
                break;
            }

            case DELETE: {
                PgMsgDelete delete_msg = std::get<PgMsgDelete>(msg.msg);
                ss << "\nDELETE";
                if (_streaming) {
                    ss << "  xid=" << delete_msg.xid << std::endl;
                }
                ss << "  rel_id=" << delete_msg.rel_id << std::endl;
                ss << "  Tuples\n";
                dump_tuple(delete_msg.tuple, ss);
                break;
            }

            case UPDATE: {
                PgMsgUpdate update = std::get<PgMsgUpdate>(msg.msg);
                ss << "\nUPDATE";
                if (_streaming) {
                    ss << "  xid=" << update.xid << std::endl;
                }
                ss << "  rel_id=" << update.rel_id << std::endl;
                ss << "  Old tuples" << std::endl;
                dump_tuple(update.old_tuple, ss);
                ss << "  New tuples" << std::endl;
                dump_tuple(update.new_tuple, ss);
                break;
            }

            case TRUNCATE: {
                PgMsgTruncate truncate = std::get<PgMsgTruncate>(msg.msg);
                ss << "\nTRUNCATE" << std::endl;
                if (_streaming) {
                    ss << "  xid=" << truncate.xid << std::endl;
                }
                for (int32_t rel_id: truncate.rel_ids) {
                    ss << "  rel_id=" << rel_id << std::endl;
                }
                break;
            }

            case ORIGIN: {
                PgMsgOrigin origin = std::get<PgMsgOrigin>(msg.msg);
                ss << "\nORIGIN" << std::endl;
                ss << "  commit LSN=" << origin.commit_lsn
                   << " (" << lsn_to_str(origin.commit_lsn) << ")\n";
                ss << "  name=" << origin.name_str << std::endl;
                break;
            }

            case MESSAGE: {
                PgMsgMessage message = std::get<PgMsgMessage>(msg.msg);
                ss << "\nMESSAGE" << std::endl;
                if (_streaming) {
                    ss << "  xid=" << message.xid << std::endl;
                }
                ss << "  LSN=" << message.lsn
                   << " (" << lsn_to_str(message.lsn) << ")\n";
                ss << "  prefix=" << message.prefix_str << std::endl;
                std::string data_str(message.data, message.data_len);
                ss << "  message=" << data_str << std::endl;
                break;
            }

            case TYPE: {
                PgMsgType type = std::get<PgMsgType>(msg.msg);
                ss << "\nTYPE" << std::endl;
                if (_streaming) {
                    ss << "  xid=" << type.xid << std::endl;
                }
                ss << "  oid=" << type.oid << std::endl;
                ss << "  namespace=" << type.namespace_str << std::endl;
                ss << "  data type=" << type.data_type_str << std::endl;
                break;
            }

            case STREAM_START: {
                PgMsgStreamStart start = std::get<PgMsgStreamStart>(msg.msg);
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
                PgMsgStreamCommit commit = std::get<PgMsgStreamCommit>(msg.msg);
                ss << "\nSTREAM COMMIT" << std::endl;
                ss << "  xid=" << commit.xid << std::endl;
                ss << "  commit LSN=" << commit.commit_lsn
                   << " (" << lsn_to_str(commit.commit_lsn) << ")\n";
                ss << "  xact LSN=" << commit.xact_lsn
                   << " (" << lsn_to_str(commit.xact_lsn) << ")\n";
                break;
            }

            case STREAM_ABORT: {
                PgMsgStreamAbort abort = std::get<PgMsgStreamAbort>(msg.msg);
                ss << "\nSTREAM ABORT" << std::endl;
                ss << "  xid=" << abort.xid << std::endl;
                ss << "  sub_xid=" << abort.sub_xid << std::endl;
                break;
            }

            case CREATE_TABLE: {
                PgMsgTable table = std::get<PgMsgTable>(msg.msg);
                ss << "\nCREATE TABLE" << std::endl;
                if (_streaming) {
                    ss << "  xid=" << table.xid << std::endl;
                }
                ss << "  oid=" << table.oid << std::endl;
                ss << "  LSN=" << table.lsn << std::endl;
                ss << "  schema=" << table.schema << std::endl;
                ss << "  table=" << table.table << std::endl;
                ss << "  columns=" << table.columns.size() << std::endl;

                for (PgMsgSchemaColumn column: table.columns) {
                    ss << "  - name=" << column.column_name << std::endl;
                    ss << "  - type=" << column.udt_type << std::endl;
                    ss << "  - default=" << column.default_value.value_or("NULL") << std::endl;
                    ss << "  - is_nullable=" << column.is_nullable << std::endl;
                    ss << "  - is_pkey=" << column.is_pkey << std::endl;
                    ss << "  - position=" << column.position << std::endl;
                }

                break;
            }

            case ALTER_TABLE: {
                PgMsgTable table = std::get<PgMsgTable>(msg.msg);
                ss << "\nALTER TABLE" << std::endl;
                if (_streaming) {
                    ss << "  xid=" << table.xid << std::endl;
                }
                ss << "  oid=" << table.oid << std::endl;
                ss << "  LSN=" << table.lsn << std::endl;
                ss << "  schema=" << table.schema << std::endl;
                ss << "  table=" << table.table << std::endl;
                ss << "  columns=" << table.columns.size() << std::endl;

                for (PgMsgSchemaColumn column: table.columns) {
                    ss << "  - name=" << column.column_name << std::endl;
                    ss << "  - type=" << column.udt_type << std::endl;
                    ss << "  - default=" << column.default_value.value_or("NULL") << std::endl;
                    ss << "  - is_nullable=" << column.is_nullable << std::endl;
                    ss << "  - position=" << column.position << std::endl;
                }

                break;
            }

            case DROP_TABLE: {
                PgMsgDropTable drop_table = std::get<PgMsgDropTable>(msg.msg);
                ss << "\nDROP TABLE" << std::endl;
                if (_streaming) {
                    ss << "  xid=" << drop_table.xid << std::endl;
                }
                ss << "  oid=" << drop_table.oid << std::endl;
                ss << "  LSN=" << drop_table.lsn << std::endl;
                ss << "  schema=" << drop_table.schema << std::endl;
                ss << "  table=" << drop_table.table << std::endl;
                break;
            }

            default:
                break;
        }

        return ss.str();
    }

    void
    PgReplMsgStream::_skip_tuple()
    {
        int num_cols = _recvint16();

        for (int i = 0; i < num_cols; i++) {
            _seek_stream();
            char type = _recvint8();
            if (type == 'n' || type =='u') {
                continue;
            }
            uint32_t data_len = recvint32(*_stream);
            _current_offset += (4 + data_len);
        }
    }

    void
    PgReplMsgStream::_skip_string()
    {
        // seek to current offset
        _seek_stream();

        // need to find terminating null char
        char buffer[128];
        uint64_t str_len = 0;

        // iterate, reading in 128 characters and searching for null char
        while (true) {
	        uint64_t length = std::min((uint64_t)128, _end_offset - _current_offset);
            _stream->read(buffer, length);
            uint64_t curr_len = strnlen(buffer, length);
            str_len += curr_len;
            // curr_len == 0 if string is null, length if no null found, or number of bytes up to null char
            if (curr_len < length) {
                break;
            }
            if (length == 0) {
                // string not found in message block!
                throw PgMessageTooSmallError();
            }
        }

        _current_offset += str_len + 1; // null char is 1 more than str_len
    }

    void
    PgReplMsgStream::_skip_relation()
    {
        // 4 - transaction ID if streaming
        if (_streaming) {
            _current_offset += 4;
        }

        // 4 - oid
        _current_offset += 4;

        _skip_string(); // namespace str

        _skip_string(); // rel name str

        _current_offset++;

        int16_t num_columns = _recvint16();

        for (int i = 0; i < num_columns; i++) {
            _current_offset++;
            _skip_string();  // column name
            _current_offset += (4 + 4); // oid, type modifier
        }

        return;
    }

    void
    PgReplMsgStream::_skip_insert()
    {
        if (_streaming) {
            _current_offset += 4; // xid
        }
        _current_offset += (4 + 1); // rel id + new type flag
        _skip_tuple();
    }

    void
    PgReplMsgStream::_skip_update()
    {
        if (_streaming) {
            _current_offset += 4; // xid
        }

        _current_offset += 4; // rel_id

        _seek_stream();
        char type = _stream->get(); // old type
        if (type == 'K' || type == 'O') {
            _current_offset++;
        }
        _skip_tuple();

        _seek_stream();
        type = _stream->get(); // new type; should be N
        if (type == 'N') {
            _current_offset++;
        }
        _skip_tuple();
    }

    void
    PgReplMsgStream::_skip_delete()
    {
        if (_streaming) {
            _current_offset += 4; // xid
        }

        _current_offset += 4; // rel_id

        _seek_stream();
        char type = _stream->get(); // old type
        if (type == 'K' || type == 'O') {
            _current_offset++;
        }
        _skip_tuple();
    }

    void
    PgReplMsgStream::_skip_truncate()
    {
        if (_streaming) {
            _current_offset += 4; // xid
        }

        uint32_t num_rels = _recvint32();

        _current_offset++; // options flag

        _current_offset += (4 * num_rels); // rel ids
    }

    void
    PgReplMsgStream::_skip_type()
    {
        if (_streaming) {
            _current_offset += 4; // xid
        }

        _current_offset += 4; // oid

        _skip_string(); // namespace
        _skip_string(); // data type
    }

    void
    PgReplMsgStream::_skip_origin()
    {
        _current_offset += 8;
        _skip_string();
    }

    bool
    PgReplMsgStream::_skip_message(uint64_t &oid, uint32_t &xid)
    {
        if (_streaming) {
            xid = _recvint32();
        }

        _current_offset += (1 + 8); // flags + lsn

        // need to decode message prefix to get oid
        char buffer[128];
        uint64_t length = std::min((uint64_t)128, _end_offset - _current_offset);
        _stream->read(buffer, length);

        // buffer should contain the full string, max should be about 45B, min ~24B
        uint64_t str_len = strnlen(buffer, length);
        if (str_len == length || strcmp(buffer, MSG_PREFIX_SPRINGTAIL) != 0) {
            // not a springtail message
            _skip_string(); // prefix string
            _current_offset += _recvint32(); // msg len + msg

            return false;
        }

        // found springtail message process it
        // should be in form "springtail:OP NAME:OID"
        std::vector<std::string> parts;
        common::split_string(":", buffer, parts);

        assert(parts.size() == 3);

        // extract oid
        oid = std::stoull(parts[2]);

        _current_offset += str_len + 1; // null char is 1 more than str_len

        _current_offset += _recvint32(); // msg len + msg

        return true;
    }


    void
    PgReplMsgStream::scan_log(std::shared_ptr<std::fstream> stream,
                              const std::filesystem::path &path,
                              uint64_t offset, uint64_t size,
                              int proto_version,
                              PgTransactionVectorPtr committed_xacts)
    {
        _current_path = path;
        _current_offset = offset;
        _end_offset = offset + size;

        set_proto_version(proto_version);

        if (!_stream->is_open()) {
            throw PgIOError();
        }

        while (_current_offset < _end_offset) {
            _scan_message(committed_xacts);
        }

        return;
    }

    void
    PgReplMsgStream::_scan_message(PgTransactionVectorPtr committed_xacts)
    {
        uint64_t start_offset = _current_offset;

        // first byte is opcode
        char msg_type = _recvint8();

        switch(msg_type) {

            // V1 Protocol
            case MSG_BEGIN: { // begin
                PgTransactionPtr xact = std::make_shared<PgTransaction>();
                xact->begin_path = _current_path;
                xact->begin_offset = _current_offset;

                char buffer[LEN_BEGIN];
                _read_buffer(buffer, LEN_BEGIN);

                set_buffer(buffer, LEN_BEGIN);
                _current_offset += decode_begin(); // returns bytes decoded

                assert(_decoded_msg.msg_type == PgReplMsgType::BEGIN);
                PgMsgBegin &begin_msg = std::get<PgMsgBegin>(_decoded_msg.msg);
                xact->xact_lsn = begin_msg.xact_lsn;
                xact->xid = begin_msg.xid;

                _current_xact = xact;
                break;
            }

            case MSG_COMMIT: { // commit
                uint64_t commit_offset = _current_offset;

                char buffer[LEN_COMMIT];
                _read_buffer(buffer, LEN_COMMIT);

                set_buffer(buffer, LEN_COMMIT);
                _current_offset += decode_commit();

                PgMsgCommit &commit_msg = std::get<PgMsgCommit>(_decoded_msg.msg);

                PgTransactionPtr xact = _current_xact;
                if (_current_xact == nullptr || commit_msg.xact_lsn != _current_xact->xact_lsn) {
                    // we don't have the start of the transaction...
                    SPDLOG_WARN("No matching xact for commit: xact_lsn={}\n", commit_msg.xact_lsn);
                    break;
                }
                xact->commit_path = _current_path;
                xact->commit_offset = commit_offset;

                committed_xacts->push_back(xact);
                _current_xact = nullptr;

                break;
            }

            case MSG_RELATION: // relation
                _skip_relation();
                break;

            case MSG_INSERT: // insert
                _skip_insert();
                break;

            case MSG_UPDATE: // update
                _skip_update();
                break;

            case MSG_DELETE: // delete
                _skip_delete();
                break;

            case MSG_TRUNCATE: // truncate
                _skip_truncate();
                break;

            case MSG_ORIGIN: // origin
                _skip_origin();
                break;

            case MSG_MESSAGE: { // message; usually a ddl command
                uint64_t oid;
                uint32_t xid;
                if (_skip_message(oid, xid)) {
                    // found a springtail message having to do with
                    // table creation, alteration, or deletion, record OID
                    if (!_streaming) {
                        // if not in streaming mode take xid from current transaction
                        xid = _current_xact->xid;
                        _current_xact->oids.insert(oid);
                    } else {
                        // in streaming mode lookup current transaction in xact map
                        auto itr = _xact_map.find(xid);
                        if (itr == _xact_map.end()) {
                            // no start streaming xact found...
                            SPDLOG_WARN("XID not found for message: xid={}\n", xid);
                        } else {
                            PgTransactionPtr xact = itr->second;
                            xact->oids.insert(oid);
                        }
                    }
                }
                break;
            }

            case MSG_TYPE: // type
                _skip_type();
                break;

            case MSG_STREAM_START: {
                uint64_t start_offset = _current_offset;

                _streaming = true;

                char buffer[LEN_STREAM_START];
                _read_buffer(buffer, LEN_STREAM_START);

                set_buffer(buffer, LEN_STREAM_START);
                _current_offset += decode_stream_start();

                PgMsgStreamStart &start_msg = std::get<PgMsgStreamStart>(_decoded_msg.msg);

                if (start_msg.first) {
                    // new transaction
                    PgTransactionPtr xact = std::make_shared<PgTransaction>();
                    xact->begin_path = _current_path;
                    xact->begin_offset = start_offset;
                    xact->xid = start_msg.xid;
                    _xact_map.insert({xact->xid, xact});
                }
                break;
            }

            case MSG_STREAM_STOP:
                _streaming = false;
                _current_offset += LEN_STREAM_STOP;
                break;

            case MSG_STREAM_COMMIT: {
                uint64_t commit_offset = _current_offset;

                char buffer[LEN_STREAM_COMMIT];
                _read_buffer(buffer, LEN_STREAM_COMMIT);

                set_buffer(buffer, LEN_STREAM_COMMIT);
                _current_offset += decode_stream_commit();

                PgMsgStreamCommit &commit_msg = std::get<PgMsgStreamCommit>(_decoded_msg.msg);

                auto itr = _xact_map.find(commit_msg.xid);
                if (itr == _xact_map.end()) {
                    // no start streaming xact found...
                    SPDLOG_WARN("No matching xact for stream commit: xid={}, xact_lsn={}",
                                commit_msg.xid, commit_msg.xact_lsn);
                    break;
                }

                PgTransactionPtr xact = itr->second;
                xact->commit_path = _current_path;
                xact->commit_offset = commit_offset;
                xact->xact_lsn = commit_msg.xact_lsn;

                _xact_map.erase(itr);

                committed_xacts->push_back(xact);
                break;
            }

            case MSG_STREAM_ABORT: {
                char buffer[LEN_STREAM_ABORT];
                _read_buffer(buffer, LEN_STREAM_ABORT);

                set_buffer(buffer, LEN_STREAM_ABORT);
                _current_offset += decode_stream_abort();

                PgMsgStreamAbort &abort_msg = std::get<PgMsgStreamAbort>(_decoded_msg.msg);

                _xact_map.erase(abort_msg.xid);

                break;
            }

            default: // unknown/unhandled
                std::cerr << "Unknown opcode to decode: " << msg_type << std::endl;
                throw PgUnknownMessageError();
        }

        // sanity check
        if (_current_offset > _end_offset) {
            std::cerr << "Buffer overrun in decode: consumed="
                      << (_current_offset - start_offset) << ", bytes available="
                      << (_end_offset - start_offset) << std::endl;

            /* Note: an error here will really require closing and re-opening the
             * replication stream to try and re-read the data */

            throw PgUnexpectedDataError();
        }
    }

}
