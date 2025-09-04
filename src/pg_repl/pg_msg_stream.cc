#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <vector>
#include <memory>

#include <absl/log/check.h>
#include <nlohmann/json.hpp>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>
#include <common/json.hh>

#include <pg_repl/pg_common.hh>
#include <pg_repl/pg_msg_stream.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/exception.hh>

extern "C" {
    #include <postgres.h>
    #include <catalog/pg_type.h>  // for BITOID, VARBITOID
}

namespace springtail {
    PgMsgStreamReader::PgMsgStreamReader(uint64_t db_id)
        :_db_id{db_id}
    {}

    PgMsgStreamReader::PgMsgStreamReader(std::optional<uint64_t> db_id,
            const std::filesystem::path &start_file,
            uint64_t start_offset)
        : _db_id(db_id),
        _current_path(start_file),
        _current_offset(0)
    {
        _open_file(start_file, start_offset);
    }

    void
    PgMsgStreamReader::set_file(const std::filesystem::path &file,
                                uint64_t start_offset)
    {
        LOG_DEBUG(LOG_PG_REPL, "Setting file to: {}, offset: {}", file, start_offset);

        if (file != _current_path || !_stream.is_open()) {
            // file isn't currently open, so open it
            // this reads in the header and sets _current_offset
            _open_file(file, start_offset);
            return;
        }

        // resync the stream and clear errors
        _stream.sync();
        _stream.clear();

        // file was already open, so just seek to the new offset
        if (_current_offset != start_offset) {
            _seek_stream(start_offset);
        }

        LOG_DEBUG(LOG_PG_REPL, "Set file to: {}, offset: {}, xlog_msg_end_offset: {}",
                  file, _current_offset, _xlog_msg_end_offset);
    }

    void
    PgMsgStreamReader::_open_file(const std::filesystem::path &file, uint64_t offset)
    {
        if (_stream.is_open()) {
            _stream.close();
        }

        _stream.open(file, std::fstream::in | std::fstream::binary);
        if (!_stream.is_open()) {
            LOG_ERROR("Unable to open file: {}", file);
            throw PgIOError();
        }
        _current_path = file;
        _current_offset = 0;
        _header = PgMsgStreamHeader();

        if (offset > _current_offset) {
            _seek_stream(offset);
        }

        LOG_DEBUG(LOG_PG_REPL, "Opened file: {}, offset: {}, xlog_msg_end_offset: {}",
                  file, _current_offset, _xlog_msg_end_offset);
    }

    void
    PgMsgStreamReader::_seek_stream(uint64_t file_offset)
    {
        LOG_DEBUG(LOG_PG_REPL, "Seeking to offset: {}, current_offset: {}, xlog_msg_end_offset: {}",
                  file_offset, _current_offset, _xlog_msg_end_offset);

        // jumping to next message, read in xlog header
        if (file_offset == _xlog_msg_end_offset) {
            if (_current_offset != _xlog_msg_end_offset) {
                // seeking to end of current xlog message, just seek to it
                _stream.seekg(_xlog_msg_end_offset, std::fstream::beg);
                _check_fail();
                _current_offset = _xlog_msg_end_offset;
            }

            // no need to read the next header here, the next read_message will do that
            // and we may be at the end of the file, which would complicate things
            return;
        }

        // nothing to do if we are already at the desired offset
        if (file_offset == _current_offset) {
            return;
        }

        // check if we are seeking back to the start of prev header
        // we don't support seeking back before the start of the current header
        // we shouldn't encounter this.
        if (file_offset < _current_offset) {
            if (file_offset == _header.header_offset) {
                // set the stream to just after the header
                if (_current_offset != _header.header_offset + PgMsgStreamHeader::SIZE) {
                    _stream.seekg(_header.header_offset + PgMsgStreamHeader::SIZE, std::fstream::beg);
                    _check_fail();
                    _current_offset = _header.header_offset + PgMsgStreamHeader::SIZE;
                }
                // otherwise current offset is already just after the header
                return;
            }
            CHECK(false) << "Seeking backwards not supported: current_offset=" << _current_offset << ", seek_offset=" << file_offset;
        }

        // if new offset is after end of current xlog message
        // we need to jump forward by reading messages until we get there
        while (file_offset >= _xlog_msg_end_offset) {
            // seek to the next xlog message header
            if (_current_offset != _xlog_msg_end_offset) {
                DCHECK_LT(_current_offset, _xlog_msg_end_offset);
                _stream.seekg(_xlog_msg_end_offset, std::fstream::beg);
                _check_fail();
                _current_offset = _xlog_msg_end_offset;
                _check_fail();
            }

            // read the header of the next message
            auto rc = _read_header();
            CHECK(rc) << "Hit EOF reading header when seeking, path="
                << _current_path << ", offset=" << _current_offset;

            if (file_offset <= _current_offset) {
                // we've jumped to or just past the desired offset
                // if we are past it is due to the header
                // check that we aren't in the middle of a header
                DCHECK(file_offset == _current_offset || file_offset == _current_offset - PgMsgStreamHeader::SIZE);
                return;
            }

            // check if the desired offset is within this message
            if (file_offset < _xlog_msg_end_offset) {
                break;
            }
        }

        DCHECK_LT(file_offset, _xlog_msg_end_offset);

        // we've jumped past the desired offset, so seek back to it
        _stream.seekg(file_offset, std::fstream::beg);
        _check_fail();
        _current_offset = file_offset;

        return;
    }

    bool
    PgMsgStreamReader::_read_header()
    {
        if (!_stream.is_open()) {
            LOG_ERROR("Stream is not open, cannot read header");
            throw PgIOError();
        }

        // read the header
        char header_buffer[PgMsgStreamHeader::SIZE];
        _stream.read(header_buffer, PgMsgStreamHeader::SIZE);
        if (_stream.eof()) {
            // hit eof
            LOG_DEBUG(LOG_PG_REPL, "Hit EOF reading header, path={}, offset={}",
                      _current_path, _current_offset);
            return false;
        }
        DCHECK_EQ(_stream.gcount(), PgMsgStreamHeader::SIZE)
            << "Failed to read full header, read " << _stream.gcount() << " bytes";

        _check_fail();

        _header = PgMsgStreamHeader(header_buffer);
        _header.header_offset = _current_offset;
        _current_offset += PgMsgStreamHeader::SIZE;

        // check if the header is valid
        if (_header.magic != PgMsgStreamHeader::MAGIC) {
            LOG_ERROR("Invalid stream header magic: {:#X}", _header.magic);
            throw PgMessageError();
        }

        // set the end message offset
        _xlog_msg_end_offset = _header.msg_length + _current_offset;

        LOG_DEBUG(LOG_PG_REPL, "Read header: {}, current_offset: {}, xlog_msg_end_offset: {}",
                  _header.to_string(),  _current_offset, _xlog_msg_end_offset);

        return true;
    }

    PgMsgPtr
    PgMsgStreamReader::read_message(const std::vector<char> &filter,
                                    bool &eos)
    {
        PgMsgPtr msg = read_message(filter);
        eos = end_of_stream();
        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::read_message(const std::vector<char> &filter)
    {
        LOG_DEBUG(LOG_PG_REPL, "Reading message, current_offset: {}, xlog_msg_end_offset: {}",
                  _current_offset, _xlog_msg_end_offset);

        // check if we've already encountered the end of the file
        if (end_of_stream()) {
            return nullptr;
        }

        if (_current_offset == _xlog_msg_end_offset || _current_offset == 0) {
            // we've reached the end of the current xlog message, so read the next header
            if (!_read_header()) {
                return nullptr;
            }
        }

        // record the message offset
        _message_offset = _current_offset;

        try {
            // read the message type
            char msg_type = _recvint8();
            bool skip_msg = !_is_message_filtered(msg_type, filter);
            PgMsgPtr msg = nullptr;

            LOG_DEBUG(LOG_PG_LOG_MGR, "Reading message type: {}, current_offset: {}, skip_msg: {}",
                      msg_type, _current_offset, skip_msg);

            if (skip_msg) {
                _skip_msg(msg_type);
            } else {
                // current streaming state; may be reset by msg parsing
                bool is_streaming = _streaming;

                // decode message
                msg = _decode_msg(msg_type);
                if (msg != nullptr) {
                    msg->proto_version = _proto_version;
                    msg->is_streaming = is_streaming;
                }
            }

            return msg;
        } catch (PgMessageEOFError &e) {
            LOG_WARN("Unexpected EOF while reading message, path={}, eos={}", _current_path, end_of_stream());
            return nullptr;
        }
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_msg(char msg_type)
    {
        switch(msg_type) {
            case pg_msg::MSG_BEGIN:
                return _decode_begin();
            case pg_msg::MSG_COMMIT:
                return _decode_commit();
            case pg_msg::MSG_RELATION:
                return _decode_relation();
            case pg_msg::MSG_INSERT:
                return _decode_insert();
            case pg_msg::MSG_UPDATE:
                return _decode_update();
            case pg_msg::MSG_DELETE:
                return _decode_delete();
            case pg_msg::MSG_TRUNCATE:
                return _decode_truncate();
            case pg_msg::MSG_TYPE:
                return _decode_type();
            case pg_msg::MSG_ORIGIN:
                return _decode_origin();
            case pg_msg::MSG_MESSAGE:
                return _decode_message();
            case pg_msg::MSG_STREAM_START:
                return _decode_stream_start();
            case pg_msg::MSG_STREAM_STOP:
                return _decode_stream_stop();
            case pg_msg::MSG_STREAM_COMMIT:
                return _decode_stream_commit();
            case pg_msg::MSG_STREAM_ABORT:
                return _decode_stream_abort();
            default:
                LOG_WARN("Unknown message type: {}", msg_type);
                DCHECK(false) << "Unknown message type";
                throw PgMessageError();
        }
    }

    void
    PgMsgStreamReader::_skip_msg(char msg_type)
    {
        switch(msg_type) {
            case pg_msg::MSG_BEGIN:
                return _skip_begin();
            case pg_msg::MSG_COMMIT:
                return _skip_commit();
            case pg_msg::MSG_RELATION:
                return _skip_relation();
            case pg_msg::MSG_INSERT:
                return _skip_insert();
            case pg_msg::MSG_UPDATE:
                return _skip_update();
            case pg_msg::MSG_DELETE:
                return _skip_delete();
            case pg_msg::MSG_TRUNCATE:
                return _skip_truncate();
            case pg_msg::MSG_TYPE:
                return _skip_type();
            case pg_msg::MSG_ORIGIN:
                return _skip_origin();
            case pg_msg::MSG_MESSAGE:
                return _skip_message();
            case pg_msg::MSG_STREAM_START:
                return _skip_stream_start();
            case pg_msg::MSG_STREAM_STOP:
                return _skip_stream_stop();
            case pg_msg::MSG_STREAM_COMMIT:
                return _skip_stream_commit();
            case pg_msg::MSG_STREAM_ABORT:
                return _skip_stream_abort();
            default:
                LOG_WARN("Unknown message type: {}", msg_type);
                throw PgMessageError();
        }
    }

    // if msg_type is not in filter then skip message
    bool
    PgMsgStreamReader::_is_message_filtered(char msg_type, const std::vector<char> &filter) const {
        for (auto c : filter) {
            if (msg_type == c) {
                return true;
            }
        }
        return false;
    }

    void
    PgMsgStreamReader::_skip_tuple()
    {
        int num_cols = _recvint16();

        for (int i = 0; i < num_cols; i++) {
            char type = _recvint8();
            if (type == 'n' || type =='u') {
                continue;
            }
            uint32_t data_len = _recvint32();
            _seek_stream(_current_offset + data_len);
        }
    }

    void
    PgMsgStreamReader::_decode_tuple(PgMsgTupleData &tuple)
    {
        int num_columns = _recvint16();
        tuple.tuple_data.resize(num_columns);

        for (int i = 0; i < num_columns; i++) {
            tuple.tuple_data[i].type = _recvint8();
            if (tuple.tuple_data[i].type == 'n' ||
                tuple.tuple_data[i].type == 'u') {
                continue;
            }

            assert(tuple.tuple_data[i].type == 't' || tuple.tuple_data[i].type == 'b');

            int32_t data_len = _recvint32();
            tuple.tuple_data[i].data.resize(data_len);

            _read_buffer(tuple.tuple_data[i].data.data(), data_len);
        }
    }

    void
    PgMsgStreamReader::_skip_string()
    {
        // Read characters until null terminator
        while (_recvint8() != '\0') {}
    }

    void
    PgMsgStreamReader::_decode_string(std::string &ostring)
    {
        char next_char;
        // Read characters until null terminator
        while ((next_char = _recvint8()) != '\0') {
            ostring.push_back(next_char);
        }
    }

    void
    PgMsgStreamReader::_skip_relation()
    {
        // 4 - transaction ID if streaming
        if (_streaming) {
            _seek_stream(_current_offset + 4);
        }

        // 4 - oid
        _seek_stream(_current_offset + 4);
        _skip_string(); // namespace str
        _skip_string(); // rel name str
        _seek_stream(_current_offset + 1);

        int16_t num_columns = _recvint16();
        for (int i = 0; i < num_columns; i++) {
            _seek_stream(_current_offset + 1);
            _skip_string();  // column name
            _seek_stream(_current_offset + 4 + 4); // oid, type modifier
        }
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_relation()
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

        PgMsgRelation relation;

        if (_streaming) {
            relation.xid = _recvint32();
        }

        relation.rel_id = _recvint32();
        _decode_string(relation.namespace_str);
        _decode_string(relation.rel_name_str);

        relation.identity = _recvint8();
        int num_columns = _recvint16();
        relation.columns.resize(num_columns);

        for (int i = 0; i < num_columns; i++) {
            relation.columns[i].flags = _recvint8();
            _decode_string(relation.columns[i].column_name);
            relation.columns[i].oid = _recvint32();
            relation.columns[i].type_modifier = _recvint32();
        }

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::RELATION);
        msg->msg.emplace<PgMsgRelation>(relation);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_insert()
    {
        if (_streaming) {
            _seek_stream(_current_offset + 4); // xid
        }
        _seek_stream(_current_offset + (4 + 1)); // rel id + new type flag
        _skip_tuple();
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_insert()
    {
        /*
            Byte1('I')  Identifies the message as an insert message.
            Int32 ID of the relation corresponding to the ID in the relation message
            Int32 XID present since version 2 (PG14)
            Byte1('N') Identifies the following TupleData message as a new tuple.
            TupleData TupleData message part representing the contents of new tuple.
        */

        PgMsgInsert insert;

        if (_streaming) {
            insert.xid = _recvint32(); // only present in v2
        }
        insert.rel_id = _recvint32();
        insert.new_type = _recvint8(); // should be 'N

        _decode_tuple(insert.new_tuple);
        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::INSERT);
        msg->msg.emplace<PgMsgInsert>(insert);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_update()
    {
        if (_streaming) {
            _seek_stream(_current_offset + 4); // xid
        }

        _seek_stream(_current_offset + 4); // rel_id

        char type = _recvint8(); // old type
        if (type == 'K' || type == 'O') {
            _skip_tuple();
            type = _recvint8(); // new type; should be N
        } else {
            CHECK_EQ(type, 'N');

        }
        _skip_tuple(); // New tuple
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_update()
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

        PgMsgUpdate update;

        if (_streaming) {
            update.xid = _recvint32();
        }
        update.rel_id = _recvint32();

        update.old_type = _recvint8();
        if (update.old_type == 'K' || update.old_type == 'O') {
            _decode_tuple(update.old_tuple);
            update.new_type = _recvint8();
        } else {
            update.new_type = update.old_type;
            update.old_type = {};
        }

        CHECK_EQ(update.new_type, 'N');
        _decode_tuple(update.new_tuple);

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::UPDATE);
        msg->msg.emplace<PgMsgUpdate>(update);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_delete()
    {
        if (_streaming) {
            _seek_stream(_current_offset + 4); // xid
        }

        _seek_stream(_current_offset + 4); // rel_id

        char type = _recvint8(); // old type
        CHECK(type == 'K' || type == 'O') << "type: " << type;
        _skip_tuple();
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_delete()
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

        PgMsgDelete delete_msg;

        if (_streaming) {
            delete_msg.xid = _recvint32();
        }
        delete_msg.rel_id = _recvint32();
        delete_msg.type = _recvint8();
        assert(delete_msg.type == 'K' || delete_msg.type == 'O');
        _decode_tuple(delete_msg.tuple);

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::DELETE);
        msg->msg.emplace<PgMsgDelete>(delete_msg);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_truncate()
    {
        if (_streaming) {
            _seek_stream(_current_offset + 4); // xid
        }

        uint32_t num_rels = _recvint32();
        _seek_stream(_current_offset + 1 + (4 * num_rels)); //options flag + rel ids
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_truncate()
    {
        /*
            Byte1('T')      Identifies the message as a truncate message.
            Int32           XID present since version 2 (PG14)
            Int32           Number of relations
            Int8            Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
            Int32           ID of the relation corresponding to the ID in the relation message.
                            This field is repeated for each relation.
        */

        PgMsgTruncate truncate;

        if (_streaming) {
            truncate.xid = _recvint32();
        }

        truncate.num_rels = _recvint32();
        truncate.options = _recvint8();
        truncate.rel_ids.resize(truncate.num_rels);
        for (int i = 0; i < truncate.num_rels; i++) {
            truncate.rel_ids[i] = _recvint32();
        }

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::TRUNCATE);
        msg->msg.emplace<PgMsgTruncate>(truncate);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_type()
    {
        if (_streaming) {
            _seek_stream(_current_offset + 4); // xid
        }
        _seek_stream(_current_offset + 4); // oid

        _skip_string(); // namespace
        _skip_string(); // data type
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_type()
    {
        /*
            Byte1('Y') Identifies the message as a type message.
            Int32 (TransactionId) Xid of the transaction (only present for streamed transactions).
                  This field is available since protocol version 2.
            Int32 (Oid) OID of the data type.
            String Namespace (empty string for pg_catalog).
            String Name of the data type.
        */

        PgMsgType type;

        if (_streaming) {
            type.xid = _recvint32();
        }

        type.oid = _recvint32();
        _decode_string(type.namespace_str);
        _decode_string(type.data_type_str);

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::TYPE);
        msg->msg.emplace<PgMsgType>(type);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_origin()
    {
        _seek_stream(_current_offset + 8);
        _skip_string();
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_origin()
    {
        /*
            Byte1('O') Identifies the message as an origin message.
            Int64 (XLogRecPtr) The LSN of the commit on the origin server.
            String Name of the origin.

            Note that there can be multiple Origin messages inside a single transaction.
        */

        PgMsgOrigin origin;
        origin.commit_lsn = _recvint64();
        _decode_string(origin.name_str);

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::ORIGIN);
        msg->msg.emplace<PgMsgOrigin>(origin);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_begin()
    {
        _seek_stream(_current_offset + LEN_BEGIN);
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_begin()
    {
        /*
            Byte1('B') Identifies the message as a begin message.
            Int64 The final LSN of the transaction.
            Int64 Commit timestamp of the transaction. Number of microseconds since Y2K
            Int32 Xid of the transaction.
        */

        PgMsgBegin begin;
        begin.xact_lsn = _recvint64();
        begin.commit_ts = _recvint64();
        begin.xid = _recvint32();

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::BEGIN);
        msg->msg.emplace<PgMsgBegin>(begin);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_commit()
    {
        _seek_stream(_current_offset + LEN_COMMIT);
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_commit()
    {
        /*
            Byte1('C') Identifies the message as a commit message.
            Int8 Flags; currently unused (must be 0).
            Int64 The LSN of the commit.
            Int64 The end LSN of the transaction.
            Int64 Commit timestamp of the transaction. Number of microseconds since Y2K
        */

        PgMsgCommit commit;

        int8_t flags = _recvint8();
        CHECK_EQ(flags, 0);

        commit.commit_lsn = _recvint64();
        commit.xact_lsn = _recvint64();
        commit.commit_ts = _recvint64();

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::COMMIT);
        msg->msg.emplace<PgMsgCommit>(commit);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_stream_start()
    {
        _seek_stream(_current_offset + LEN_STREAM_START);

        _streaming = true;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_stream_start()
    {
        /*
            Byte1('S')  Identifies the message as a stream start message.
            Int32       Xid of the transaction.
            Int8_t      A value of 1 indicates this is the first stream segment for this XID, 0 for any other stream segment.
        */

        PgMsgStreamStart start;

        start.xid = _recvint32();
        start.first = (_recvint8() == 1);

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::STREAM_START);
        msg->msg.emplace<PgMsgStreamStart>(start);

        _streaming = true;

        return msg;
    }

    void
    PgMsgStreamReader::_skip_stream_stop()
    {
        _seek_stream(_current_offset + LEN_STREAM_STOP);

        _streaming = false;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_stream_stop()
    {
        /*
            Byte1('E')  Identifies the message as a stream stop message.
        */
        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::STREAM_STOP);
        _streaming = false;
        return msg;
    }

    void
    PgMsgStreamReader::_skip_stream_commit()
    {
        _seek_stream(_current_offset + LEN_STREAM_COMMIT);
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_stream_commit()
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
        PgMsgStreamCommit commit;

        commit.xid = _recvint32();
        _recvint8(); // flags
        commit.commit_lsn = _recvint64();
        commit.xact_lsn = _recvint64();
        commit.commit_ts = _recvint64();

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::STREAM_COMMIT);
        msg->msg.emplace<PgMsgStreamCommit>(commit);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_stream_abort()
    {
        uint64_t new_offset = _current_offset += 8; // xid + sub_xid
        if (_proto_version >= 4) {
            new_offset += (8 + 8); // abort_lsn + abort_ts
        }
        _seek_stream(new_offset);
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_stream_abort()
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
        PgMsgStreamAbort abort;

        abort.xid = _recvint32();
        abort.sub_xid = _recvint32();
        if (_proto_version >= 4) {
            abort.abort_lsn = _recvint64();
            abort.abort_ts = _recvint64();
        }

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::STREAM_ABORT);
        msg->msg.emplace<PgMsgStreamAbort>(abort);

        return msg;
    }

    void
    PgMsgStreamReader::_decode_schema_columns(const nlohmann::json &column_json,
                                              std::vector<PgMsgSchemaColumn> &columns)
    {
        // iterate through json array
        for (auto &el: column_json.items()) {
            PgMsgSchemaColumn column;
            nlohmann::json json = el.value();

            json["name"].get_to(column.name);
            json["position"].get_to(column.position);
            json["pg_type"].get_to(column.pg_type);
            json["is_nullable"].get_to(column.is_nullable);
            json["is_pkey"].get_to(column.is_pkey);
            json["is_generated"].get_to(column.is_generated);
            json["type_name"].get_to(column.type_name);
            json["type_namespace"].get_to(column.type_namespace);

            // SPR-774
            if (column.pg_type == BITOID) {
                column.pg_type = VARBITOID;
            }

            if (!json["collation"].is_null()) {
                column.collation = json["collation"].get<std::string>();
            }

            json["is_non_standard_collation"].get_to(column.is_non_standard_collation);
            json["is_user_defined_type"].get_to(column.is_user_defined_type);
            Json::get_to<char>(json, "type_category", column.type_category);

            if (!json["pkey_pos"].is_null()) {
                json["pkey_pos"].get_to(column.pk_position);
            } else {
                column.pk_position = -1;
            }

            if (!json["default"].is_null()) {
                column.default_value = json["default"].get<std::string>();
            }

            column.type = static_cast<uint8_t>(convert_pg_type(column.pg_type, column.type_category));
            columns.push_back(column);
        }
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_create_index(const PgMsgMessage &message, char *buffer, int len) {
        // convert msg data to string (it is not null terminated)
        // and convert string to json
        std::string data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        PgMsgIndex msg;

        std::string object_type;
        json["obj"].get_to(object_type);
        if (object_type != "index") {
            LOG_INFO("Create index msg not for index object, for: {}\n", object_type);
            CHECK_EQ(object_type, "index");
            return {};
        }

        msg.xid = message.xid; // only valid in streaming mode
        msg.lsn = message.lsn;
        json["oid"].get_to(msg.oid);
        json["schema"].get_to(msg.namespace_name);
        json["table_name"].get_to(msg.table_name);

        if (!_is_schema_included(msg.namespace_name)) {
            LOG_INFO("Create index skipped: {} {}\n", msg.table_name, msg.namespace_name);
            return {};
        }

        json["table_oid"].get_to(msg.table_oid);
        json["is_unique"].get_to(msg.is_unique);
        json["identity"].get_to(msg.index);

        const nlohmann::json& cols = json["columns"];
        for (const auto &el: cols.items()) {
            PgMsgSchemaIndexColumn col;
            const auto& v  = el.value();
            v["name"].get_to(col.name);
            v["position"].get_to(col.position);
            v["idx_position"].get_to(col.idx_position);
            msg.columns.push_back(col);
        }

        PgMsgPtr decoded_msg = std::make_shared<PgMsg>(PgMsgEnum::CREATE_INDEX);
        decoded_msg->msg.emplace<PgMsgIndex>(msg);

        return decoded_msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_drop_index(const PgMsgMessage &message, char *buffer, int len) {
        std::string data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        PgMsgDropIndex msg;

        std::string object_type;
        json["obj"].get_to(object_type);
        if (object_type != "index") {
            LOG_INFO("Create index msg not for index object, for: {}\n", object_type);
            return {};
        }

        msg.xid = message.xid; // only valid in streaming mode
        msg.lsn = message.lsn;
        json["schema"].get_to(msg.namespace_name);
        json["oid"].get_to(msg.oid);
        json["identity"].get_to(msg.index);

        if (!_is_schema_included(msg.namespace_name)) {
            LOG_INFO("Create index skipped: {} {}\n", msg.oid, msg.namespace_name);
            return {};
        }

        PgMsgPtr decoded_msg = std::make_shared<PgMsg>(PgMsgEnum::DROP_INDEX);
        decoded_msg->msg.emplace<PgMsgDropIndex>(msg);

        return decoded_msg;
    }

    PartitionData
    _decode_partition_data(const nlohmann::json &partition_data){
        PartitionData data;
        data.table_id = partition_data["table_id"];
        data.namespace_id = partition_data["namespace_id"];
        data.table_name = partition_data["table_name"];
        if (partition_data.contains("partition_key") && !partition_data["partition_key"].is_null()) {
            data.partition_key = partition_data["partition_key"];
        }
        if (partition_data.contains("partition_bound") && !partition_data["partition_bound"].is_null()) {
            data.partition_bound = partition_data["partition_bound"];
        }
        data.parent_table_id = partition_data["parent_table_id"];
        return data;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_create_table(PgMsgMessage &message, char *buffer, int len)
    {
        PgMsgTable table_msg;

        // convert msg data to string (it is not null terminated)
        // and convert string to json
        std::string data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        LOG_DEBUG(LOG_PG_LOG_MGR, "Decoded create table: json: {}", json.dump());

        // check object type, could be an index, default value or something other
        // than a table
        std::string object_type;
        json["obj"].get_to(object_type);
        if (object_type != "table") {
            LOG_INFO("Create/alter table msg not for table object, for: {}\n", object_type);
            return nullptr;
        }

        table_msg.xid = message.xid; // only valid in streaming mode
        table_msg.lsn = message.lsn;

        json["table"].get_to(table_msg.table);
        json["schema"].get_to(table_msg.namespace_name);

        //check include schemas
        if (!_is_schema_included(table_msg.namespace_name)) {
            LOG_INFO("Create table skipped: {} {}\n", table_msg.table, table_msg.namespace_name);
            return {};
        }

        DCHECK(json.contains("schema_id"));
        json["schema_id"].get_to(table_msg.namespace_id);

        json["oid"].get_to(table_msg.oid);
        json["rls_enabled"].get_to(table_msg.rls_enabled);
        json["rls_forced"].get_to(table_msg.rls_forced);
        if (!json["parent_table_id"].is_null()) {
            json["parent_table_id"].get_to(table_msg.parent_table_id);
        } else {
            table_msg.parent_table_id = 0;
        }
        if (!json["partition_key"].is_null()) {
            json["partition_key"].get_to(table_msg.partition_key);
        }
        if (!json["partition_bound"].is_null()) {
            json["partition_bound"].get_to(table_msg.partition_bound);
        }

        for (const auto &partition_data : json["partition_data"]) {
            table_msg.partition_data.push_back(_decode_partition_data(partition_data));
        }

        _decode_schema_columns(json["columns"], table_msg.columns);

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::CREATE_TABLE);
        msg->msg.emplace<PgMsgTable>(table_msg);

        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_alter_table(PgMsgMessage &message, char *buffer, int len)
    {
        // same data as in create table, call that to do the decode and
        // then just switch the type so we know it is an alter table
        PgMsgPtr msg = _decode_create_table(message, buffer, len);
        if (msg == nullptr) {
            return msg;
        }

        msg->msg_type = PgMsgEnum::ALTER_TABLE;
        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_drop_table(PgMsgMessage &message, char *buffer, int len)
    {
        PgMsgDropTable drop_table_msg;
        std::string data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        LOG_DEBUG(LOG_PG_LOG_MGR, "Decoded drop table: json: {}", json.dump());

        // check object type, could be an index, default value or something other
        // than a table; if so we skip decoding
        std::string object_type;
        json["obj"].get_to(object_type);
        if (object_type != "table") {
            LOG_INFO("Drop table not for table object, for: {}\n", object_type);
            CHECK_EQ(object_type, "table");
            return nullptr;
        }

        drop_table_msg.xid = message.xid; // only valid in streaming mode
        drop_table_msg.lsn = message.lsn;

        json["oid"].get_to(drop_table_msg.oid);
        json["schema"].get_to(drop_table_msg.namespace_name);

        //check include schemas
        if (!_is_schema_included(drop_table_msg.namespace_name)) {
            LOG_INFO("Drop table skipped: {} {}\n", drop_table_msg.table, drop_table_msg.namespace_name);
            return {};
        }

        json["name"].get_to(drop_table_msg.table);

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::DROP_TABLE);
        msg->msg.emplace<PgMsgDropTable>(drop_table_msg);

        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_create_namespace(PgMsgMessage &message, char *buffer, int len)
    {
        PgMsgNamespace ns_msg;

        // convert msg data to string (it is not null terminated)
        // and convert string to json
        std::string_view data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        // check object type, should be of type namespace
        std::string object_type;
        json["obj"].get_to(object_type);
        if (object_type != "schema") {
            LOG_ERROR("Create/alter namespace msg not for namespace object, for: {}\n", object_type);
            return nullptr;
        }

        ns_msg.xid = message.xid; // only valid in streaming mode
        ns_msg.lsn = message.lsn;
        json["name"].get_to(ns_msg.name);
        json["oid"].get_to(ns_msg.oid);

        LOG_DEBUG(LOG_PG_LOG_MGR, "Decoded create/alter namespace: json: {}", json.dump());

        //check include schemas
        if (!_is_schema_included(ns_msg.name)) {
            LOG_INFO("Create namespace skipped: {}\n", ns_msg.name);
            return {};
        }

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::CREATE_NAMESPACE);
        msg->msg.emplace<PgMsgNamespace>(ns_msg);

        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_alter_namespace(PgMsgMessage &message, char *buffer, int len)
    {
        // same data as in create namespace, call that to do the decode and then just switch the
        // type so we know it is an alter namespace
        PgMsgPtr msg = _decode_create_namespace(message, buffer, len);
        if (msg == nullptr) {
            return msg;
        }

        msg->msg_type = PgMsgEnum::ALTER_NAMESPACE;
        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_drop_namespace(PgMsgMessage &message, char *buffer, int len)
    {
        PgMsgNamespace ns_msg;

        // convert msg data to string (it is not null terminated)
        // and convert string to json
        std::string data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        // check object type, should be of type namespace
        std::string object_type;
        json["obj"].get_to(object_type);
        if (object_type != "schema") {
            LOG_ERROR("Drop namespace msg not for namespace object, for: {}\n", object_type);
            return nullptr;
        }

        ns_msg.xid = message.xid; // only valid in streaming mode
        ns_msg.lsn = message.lsn;
        json["oid"].get_to(ns_msg.oid);
        json["name"].get_to(ns_msg.name);

        LOG_DEBUG(LOG_PG_LOG_MGR, "Decoded drop namespace: json: {}", json.dump());

        //check include schemas
        if (!_is_schema_included(ns_msg.name)) {
            LOG_INFO("Drop namespace skipped: {}\n", ns_msg.name);
            return {};
        }

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::DROP_NAMESPACE);
        msg->msg.emplace<PgMsgNamespace>(ns_msg);

        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_create_usertype(const PgMsgMessage &message, char *buffer, int len)
    {
        PgMsgUserType usertype_msg;

        // convert msg data to string (it is not null terminated)
        // and convert string to json
        std::string_view data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        usertype_msg.xid = message.xid; // only valid in streaming mode
        usertype_msg.lsn = message.lsn;

        Json::get_to<char>(json, "type", usertype_msg.type); // convert string to char
        json["name"].get_to(usertype_msg.name);
        json["oid"].get_to(usertype_msg.oid);
        json["schema"].get_to(usertype_msg.namespace_name);
        json["ns_oid"].get_to(usertype_msg.namespace_id);
        json["value"].get_to(usertype_msg.value_json);

        CHECK_EQ(usertype_msg.type, 'E');

        //check include schemas
        if (!_is_schema_included(usertype_msg.namespace_name)) {
            LOG_INFO("Create user type skipped: {}\n", usertype_msg.namespace_name);
            return {};
        }

        LOG_DEBUG(LOG_PG_LOG_MGR, "Decoded create/alter usertype: json: {}", json.dump());

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::CREATE_TYPE);
        msg->msg.emplace<PgMsgUserType>(usertype_msg);

        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_alter_usertype(const PgMsgMessage &message, char *buffer, int len)
    {
        // same data as in create usertype, call that to do the decode and then just switch the
        // type so we know it is an alter usertype
        PgMsgPtr msg = _decode_create_usertype(message, buffer, len);
        if (msg == nullptr) {
            return msg;
        }

        msg->msg_type = PgMsgEnum::ALTER_TYPE;
        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_drop_usertype(const PgMsgMessage &message, char *buffer, int len)
    {
        PgMsgUserType usertype_msg;

        // convert msg data to string (it is not null terminated)
        // and convert string to json
        std::string data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        // check object type, should be of type namespace
        std::string object_type;
        json["obj"].get_to(object_type);
        if (object_type != "type") {
            LOG_ERROR("Drop msg not for usertype object, for: {}\n", object_type);
            return nullptr;
        }

        usertype_msg.xid = message.xid; // only valid in streaming mode
        usertype_msg.lsn = message.lsn;
        json["oid"].get_to(usertype_msg.oid);
        json["name"].get_to(usertype_msg.name);
        json["schema"].get_to(usertype_msg.namespace_name);

        //check include schemas
        if (!_is_schema_included(usertype_msg.namespace_name)) {
            LOG_INFO("Create user type skipped: {}\n", usertype_msg.namespace_name);
            return {};
        }

        LOG_DEBUG(LOG_PG_LOG_MGR, "Decoded drop usertype: json: {}", json.dump());

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::DROP_TYPE);
        msg->msg.emplace<PgMsgUserType>(usertype_msg);

        return msg;
    }


    PgMsgPtr
    PgMsgStreamReader::_decode_copy_sync(const PgMsgMessage &message, char *buffer, int len)
    {
        PgMsgCopySync copy_sync_msg;
        std::string data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        json["pg_xid"].get_to(copy_sync_msg.pg_xid);
        json["target_xid"].get_to(copy_sync_msg.target_xid);

        PgMsgPtr msg = std::make_shared<PgMsg>(PgMsgEnum::COPY_SYNC);
        msg->msg.emplace<PgMsgCopySync>(copy_sync_msg);

        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_attach_partition(const PgMsgMessage &message, const char *buffer, int len)
    {
        PgMsgAttachPartition attach_partition_msg;
        std::string data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        LOG_DEBUG(LOG_PG_LOG_MGR, "Decoded attach partition: json: {}", json.dump());

        attach_partition_msg.xid = message.xid;
        attach_partition_msg.lsn = message.lsn;

        json["table_id"].get_to(attach_partition_msg.table_id);
        json["schema"].get_to(attach_partition_msg.namespace_name);
        json["table"].get_to(attach_partition_msg.table_name);
        json["partition_key"].get_to(attach_partition_msg.partition_key);

        for (const auto &partition_data : json["partition_data"]) {
            attach_partition_msg.partition_data.push_back(_decode_partition_data(partition_data));
        }

        auto msg = std::make_shared<PgMsg>(PgMsgEnum::ATTACH_PARTITION);
        msg->msg.emplace<PgMsgAttachPartition>(attach_partition_msg);

        return msg;
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_detach_partition(const PgMsgMessage &message, const char *buffer, int len)
    {
        PgMsgDetachPartition detach_partition_msg;
        std::string data_str(buffer, len);
        nlohmann::json json = nlohmann::json::parse(data_str);

        LOG_DEBUG(LOG_PG_LOG_MGR, "Decoded detach partition: json: {}", json.dump());

        detach_partition_msg.xid = message.xid;
        detach_partition_msg.lsn = message.lsn;

        json["table_id"].get_to(detach_partition_msg.table_id);
        json["schema"].get_to(detach_partition_msg.namespace_name);
        json["table"].get_to(detach_partition_msg.table_name);
        json["partition_key"].get_to(detach_partition_msg.partition_key);

        for (const auto &partition_data : json["partition_data"]) {
            detach_partition_msg.partition_data.push_back(_decode_partition_data(partition_data));
        }

        auto msg = std::make_shared<PgMsg>(PgMsgEnum::DETACH_PARTITION);
        msg->msg.emplace<PgMsgDetachPartition>(detach_partition_msg);

        return msg;
    }

    void
    PgMsgStreamReader::_skip_message()
    {
        if (_streaming) {
            _seek_stream(_current_offset + 4); // xid
        }

        _seek_stream(_current_offset + (1 + 8)); // flags + lsn
        _skip_string(); // prefix
        auto len = _recvint32();
        _seek_stream(_current_offset + len); // msg len + msg
    }

    PgMsgPtr
    PgMsgStreamReader::_decode_message()
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

        PgMsgMessage msg;

        if (_streaming) {
            msg.xid = _recvint32();
        } else {
            msg.xid = 0;
        }

        msg.flags = _recvint8();
        msg.lsn = _recvint64();
        _decode_string(msg.prefix_str);

        int data_len = _recvint32();
        std::vector<char> buffer;
        buffer.resize(data_len);
        _read_buffer(buffer.data(), data_len);

        if (msg.prefix_str == pg_msg::MSG_PREFIX_CREATE_TABLE) {
            return _decode_create_table(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_ALTER_TABLE) {
            return _decode_alter_table(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_DROP_TABLE) {
            return _decode_drop_table(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_CREATE_NAMESPACE) {
            return _decode_create_namespace(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_ALTER_NAMESPACE) {
            return _decode_alter_namespace(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_DROP_NAMESPACE) {
            return _decode_drop_namespace(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_CREATE_INDEX) {
            return _decode_create_index(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_DROP_INDEX) {
            return _decode_drop_index(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_COPY_SYNC) {
            return _decode_copy_sync(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_CREATE_TYPE) {
            return _decode_create_usertype(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_ALTER_TYPE) {
            return _decode_alter_usertype(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_DROP_TYPE) {
            return _decode_drop_usertype(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_ATTACH_PARTITION) {
            return _decode_attach_partition(msg, buffer.data(), data_len);
        } else if (msg.prefix_str == pg_msg::MSG_PREFIX_DETACH_PARTITION) {
            return _decode_detach_partition(msg, buffer.data(), data_len);
        } else {
            LOG_INFO("Unknown message prefix: {}", msg.prefix_str);
            return nullptr;
        }
    }

    uint64_t
    PgMsgStreamReader::scan_log(uint64_t db_id, const std::filesystem::path &file, bool truncate)
    {
        PgMsgStreamReader reader(db_id, file);

        auto end_lsn = reader._repair_log(file, truncate);
        LOG_INFO("Scanned file {}, found end LSN: {}", file, end_lsn);

        return end_lsn;
    }

    LSN_t
    PgMsgStreamReader::_repair_log(const std::filesystem::path &file, bool truncate)
    {
        // this replicates some login in _open_file and _seek_stream
        // but we need to handle reading the header separately

        // open file and position at start
        if (_stream.is_open()) {
            _stream.close();
        }

        _stream.open(file, std::fstream::in | std::fstream::binary);
        if (!_stream.is_open()) {
            LOG_ERROR("Unable to open file: {}", file);
            throw PgIOError();
        }
        _current_path = file;
        _current_offset = 0;

        // get file size
        auto file_end_offset = std::filesystem::file_size(file);

        // check if file too small to contain a valid header
        if (file_end_offset < PgMsgStreamHeader::SIZE) {
            if (truncate) {
                _truncate_file(file, 0);
            }
            return INVALID_LSN;
        }

        uint64_t current_lsn_msg_start = 0;
        LSN_t current_start_lsn = INVALID_LSN;
        LSN_t last_valid_start_lsn = INVALID_LSN;
        char current_msg_type = 0;

        // Make sure we have room for at least a header + type
        while (_current_offset + PgMsgStreamHeader::SIZE + 1 <= file_end_offset) {

            // read header at current offset and get next header offset
            // after reading the header, _current_offset is set to just after the header
            CHECK(_read_header());
            current_msg_type = _stream.peek();

            // check if the LSN has changed, if so record it
            if (current_start_lsn != _header.start_lsn) {
                last_valid_start_lsn = current_start_lsn;
                current_start_lsn = _header.start_lsn;
                current_lsn_msg_start = _current_offset - PgMsgStreamHeader::SIZE;
            }

            // check if the full message fits in the file
            if (_xlog_msg_end_offset == file_end_offset) {
                // last full message of the file
                 if (current_msg_type == pg_msg::MSG_COMMIT ||
                     current_msg_type == pg_msg::MSG_STREAM_COMMIT ||
                     current_msg_type == pg_msg::MSG_STREAM_ABORT) {
                    return _header.end_lsn+1;
                }
                // otherwise truncate to start of this lsn message
                break;
            }

            if (_xlog_msg_end_offset < file_end_offset) {
                // safe message, seek to next message
                _stream.seekg(_xlog_msg_end_offset, std::fstream::beg);
                _current_offset = _xlog_msg_end_offset;
                continue;
            }

            DCHECK_GT(_xlog_msg_end_offset, file_end_offset);
            // if we reach here, we have a partial message at the end of the file

            break;
        }

        if (current_lsn_msg_start == 0) {
            // need to restart from previous log file's end LSN
            // return INVALID_LSN and caller will remove file and
            // examine previous file
            return INVALID_LSN;
        }

        // partial message detected; _xlog_msg_end_offset > file_end_offset
        // truncate to just before this message header
        if (truncate) {
            _truncate_file(file, current_lsn_msg_start);
        }

        // replay this message (LSN)
        return last_valid_start_lsn;
    }

    bool
    PgMsgStreamReader::_is_schema_included(const std::string& schema)
    {
        if (!_db_id.has_value()) {
            return true;
        }
        auto included_schemas = Properties::get_include_schemas(*_db_id);
        return included_schemas.empty() ||
           std::ranges::find(included_schemas, schema) != included_schemas.end();
    }

    void
    PgMsgStreamReader::_truncate_file(const std::filesystem::path &file, uint64_t offset)
    {
        int fd = ::open(file.c_str(), O_WRONLY);
        if (fd == -1) {
            LOG_ERROR("Failed to open file {} for truncation: {}", file, errno);
            throw PgIOError();
        }

        if (::ftruncate(fd, offset) == -1) {
            LOG_ERROR("Failed to truncate file {} to offset {}: {}", file, offset, errno);
            ::close(fd);
            throw PgIOError();
        }

        ::close(fd);
    }

    //////////////////////////////////////////////////////////////////////////

    PgMsgStreamWriter::PgMsgStreamWriter(const std::filesystem::path &file)
        : _file(file)
    {
        mode_t owner = S_IRUSR | S_IWUSR | S_IRGRP;

        _fd = ::open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, owner);
        if (_fd == -1) {
            throw PgIOError();
        }
    }

    uint64_t
    PgMsgStreamWriter::write_message(const PgCopyData &data)
    {
        if (data.length == 0) {
            return _current_offset;
        }

        // write out message
        CHECK_EQ(::write(_fd, data.buffer, data.length), data.length);
        _current_offset += data.length;

        return _current_offset;
    }

    uint64_t
    PgMsgStreamWriter::write_header(const PgMsgStreamHeader &header)
    {
        // write out header
        char buffer[PgMsgStreamHeader::SIZE];
        header.encode_header(buffer);
        CHECK_EQ(::write(_fd, buffer, sizeof(buffer)), sizeof(buffer));
        _current_offset += sizeof(buffer);

        return _current_offset;
    }

    void
    PgMsgStreamWriter::sync()
    {
        ::fsync(_fd);
    }

    void
    PgMsgStreamWriter::close()
    {
        if (_fd != -1) {
            ::close(_fd);
            _fd = -1;
        }
    }

}
