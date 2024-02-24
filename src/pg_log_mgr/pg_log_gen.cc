#include <stdlib.h>
#include <stdio.h>
#include <cassert>

#include <nlohmann/json.hpp>

#include <common/common.hh>
#include <common/exception.hh>

#include <pg_log_mgr/pg_log_gen.hh>
#include <pg_log_mgr/pg_log_writer.hh>

namespace springtail {
    PgLogGen::PgLogGen(const std::filesystem::path &file_name)
    {
        _file_name = file_name;
        _fp = ::fopen(_file_name.c_str(), "w");
        if (_fp == nullptr) {
            throw Error("Failed to open file: " + _file_name.string());
        }
        // set the header offset to beginning of file; seek past header
        _header_offset = 0;
        ::fseek(_fp, PgLogWriter::PG_LOG_HDR_BYTES, SEEK_CUR);
    }

    PgLogGen::~PgLogGen()
    {
        ::fclose(_fp);
    }

    void
    PgLogGen::_write(const char *data, size_t size)
    {
        if (::fwrite(data, size, 1, _fp) < 0) {
            throw Error("Failed to write to file: " + _file_name.string());
        }

        _bytes_written += size;
        if (_bytes_written >= PG_WRAP_LSN_BYTES) {
            _bytes_written = 0;
            _lsn++;
        }
    }

    void
    PgLogGen::_write_string(const std::string &str)
    {
        _write(str.c_str(), str.size());
        _write("\0", 1);
    }

    void
    PgLogGen::_write_message(const char *prefix, const nlohmann::json &msg)
    {
        _write_uint8(PgReplMsg::MSG_MESSAGE);

        if (_is_streaming) { // since proto version 2
            _write_uint32(_xid);
        }

        // 1B flags, 8B LSN, prefix str, data len, data
        _write_uint8(0);

        _write_uint64(_lsn);
        _write_string(prefix);

        std::string json_string = msg.dump();
        _write_uint32(json_string.size());

        _write(json_string.c_str(), json_string.size());
    }

    nlohmann::json
    PgLogGen::_gen_table_schema(uint32_t table_id, const std::vector<PgMsgSchemaColumn> &columns)
    {
        nlohmann::json columns_json;

        _schema_map[table_id] = {};

        for (auto &&c: columns) {
            nlohmann::json col;
            col["name"] = c.column_name;
            col["is_nullable"] = c.is_nullable;
            col["type"] = c.udt_type;
            if (c.default_value.has_value()) {
                col["default"] = c.default_value.value();
            } else {
                col["default"] = nullptr;
            }
            col["is_pkey"] = c.is_pkey;
            col["position"] = c.position;
            columns_json.push_back(col);

            assert(c.udt_type == PG_SCHEMA_TYPE_BOOL ||
                   c.udt_type == PG_SCHEMA_TYPE_INT4 ||
                   c.udt_type == PG_SCHEMA_TYPE_INT8 ||
                   c.udt_type == PG_SCHEMA_TYPE_TEXT);

            _schema_map[table_id].push_back(c.udt_type);
            if (c.is_pkey) {
                _pkey_map[table_id].push_back(c.udt_type);
            }
        }

        return columns_json;
    }

    void
    PgLogGen::_write_tuple(uint32_t table_id,
                           const std::vector<std::string> &types,
                           const std::vector<std::string> &columns)
    {
        _write_uint16(columns.size());
        assert(types.size() == columns.size());

        for (int i = 0; i < columns.size(); i++) {
            auto &&c = columns[i];
            auto &&t = types[i];

            if (c == PG_VALUE_NULL) {
                _write_uint8('n'); // null
                continue;
            }

            _write_uint8('b'); // binary format

            if (t == PG_SCHEMA_TYPE_BOOL) {
                _write_uint32(1);
                if (c == PG_VALUE_TRUE) {
                    _write_uint8(1);
                } else {
                    _write_uint8(0);
                }
            } else if (t == PG_SCHEMA_TYPE_INT4) {
                _write_uint32(4);
                _write_uint32(std::stoi(c));
            } else if (t == PG_SCHEMA_TYPE_INT8) {
                _write_uint32(8);
                _write_uint64(std::stoll(c));
            } else if (t == PG_SCHEMA_TYPE_TEXT) {
                _write_uint32(c.size());
                _write(c.c_str(), c.size());
            } else {
                throw Error("Unknown type: " + types[i]);
            }
        }
    }

    void
    PgLogGen::_write_header()
    {
        // calculate message length
        int current_offset = ::ftell(_fp);
        int msg_length = current_offset - _header_offset - PgLogWriter::PG_LOG_HDR_BYTES;

        if (msg_length == 0) {
            return;
        }

        // seek to header offset
        ::fseek(_fp, _header_offset, SEEK_SET);

        // write header
        char buffer[PgLogWriter::PG_LOG_HDR_BYTES];
        PgLogWriter::PgLogHeader header(msg_length, _begin_lsn, _lsn, 2);
        header.encode_header(buffer);
        _write(buffer, PgLogWriter::PG_LOG_HDR_BYTES);

        // mark current offset (old offset) as new header offset; leave room for new header
        _header_offset = current_offset;
        ::fseek(_fp, current_offset + PgLogWriter::PG_LOG_HDR_BYTES, SEEK_SET);
    }

    void
    PgLogGen::_add_start_xact()
    {
        _current_xact = std::make_shared<PgReplMsgStream::PgTransaction>();
        _current_xact->xid = _xid;
        _current_xact->xact_lsn = _begin_lsn;
        _current_xact->begin_offset = ::ftell(_fp);
        _current_xact->begin_path = _file_name;
    }

    void
    PgLogGen::_add_end_xact()
    {
        _current_xact->commit_offset = ::ftell(_fp);
        _current_xact->commit_path = _file_name;
        _xact_list.push_back(_current_xact);
    }

    uint32_t
    PgLogGen::create_table(const std::string &table_name,
                           const std::vector<PgMsgSchemaColumn> &columns)
    {
        uint32_t table_id = _next_table_id++;
        _table_id_to_name[table_id] = table_name;

        nlohmann::json msg;

        msg["cmd"] = "CREATE TABLE";
        msg["oid"] = table_id;
        msg["obj"] = "table";
        msg["schema"] = "public";
        msg["columns"] = _gen_table_schema(table_id, columns);
        msg["identity"] = "public." + table_name;

        _write_message(PgReplMsg::MSG_PREFIX_CREATE_TABLE, msg);

        return table_id;
    }

    void
    PgLogGen::alter_table(uint32_t table_id, const std::vector<PgMsgSchemaColumn> &columns)
    {
        nlohmann::json msg;

        msg["cmd"] = "ALTER TABLE";
        msg["oid"] = table_id;
        msg["obj"] = "table";
        msg["schema"] = "public";
        msg["columns"] = _gen_table_schema(table_id, columns);
        msg["identity"] = "public." + _table_id_to_name[table_id];

        _write_message(PgReplMsg::MSG_PREFIX_ALTER_TABLE, msg);
    }

    void
    PgLogGen::drop_table(uint32_t table_id) {
        nlohmann::json msg;

        msg["cmd"] = "DROP TABLE";
        msg["oid"] = table_id;
        msg["obj"] = "table";
        msg["schema"] = "public";
        msg["identity"] = "public." + _table_id_to_name[table_id];

        _write_message(PgReplMsg::MSG_PREFIX_DROP_TABLE, msg);
    }

    uint32_t
    PgLogGen::begin()
    {
        _write_header(); // close previous msg, start new one

        _begin_lsn = _lsn;
        _commit_ts = get_pgtime_in_millis();

        _add_start_xact();

        // LSN, ts, xid
        _write_uint8(PgReplMsg::MSG_BEGIN);
        _write_uint64(_lsn);
        _write_uint64(_commit_ts);
        _write_uint32(_xid);

        return _xid;
    }

    void
    PgLogGen::commit()
    {
        _add_end_xact();

        // LSN, ts, xid
        _write_uint8(PgReplMsg::MSG_COMMIT);
        _write_uint8(0);
        _write_uint64(_lsn);
        // since xacts are matched based on lsn and logging begin occurs before commit,
        // we use the begin lsn as the xact lsn
        _write_uint64(_begin_lsn);
        _write_uint64(_commit_ts);

        _write_header(); // close previous msg, start new one

        // increment xid
        _xid++;
    }

    void
    PgLogGen::truncate(uint32_t table_id)
    {
        _write_uint8(PgReplMsg::MSG_TRUNCATE);
        if (_is_streaming) { // since proto version 2
            _write_uint32(_xid);
        }
        _write_uint32(1); // number of tables
        _write_uint8(1);  // options for truncate=1
        _write_uint32(table_id);
    }

    void
    PgLogGen::insert(uint32_t table_id, const std::vector<std::string> &row_columns)
    {
        _write_uint8(PgReplMsg::MSG_INSERT);
        if (_is_streaming) { // since proto version 2
            _write_uint32(_xid);
        }
        _write_uint32(table_id);
        _write_uint8('N'); // new tuple
        _write_tuple(table_id, _schema_map[table_id], row_columns);
    }

    void
    PgLogGen::update(uint32_t table_id, const std::vector<std::string> &key_columns,
                     const std::vector<std::string> &row_columns,
                     bool using_pkey)
    {
        _write_uint8(PgReplMsg::MSG_UPDATE);
        if (_is_streaming) { // since proto version 2
            _write_uint32(_xid);
        }
        _write_uint32(table_id);
        if (using_pkey) {
            _write_uint8('K'); // key tuple
        } else {
            _write_uint8('O'); // old tuple
        }
        _write_tuple(table_id, _pkey_map[table_id], key_columns);
        _write_uint8('N'); // new tuple
        _write_tuple(table_id, _schema_map[table_id], row_columns);
    }

    void
    PgLogGen::delrow(uint32_t table_id,
                     const std::vector<std::string> &key_columns,
                     bool using_pkey)
    {
        _write_uint8(PgReplMsg::MSG_DELETE);
        if (_is_streaming) { // since proto version 2
            _write_uint32(_xid);
        }
        _write_uint32(table_id);
        if (using_pkey) {
            _write_uint8('K'); // key tuple
        } else {
            _write_uint8('O'); // old tuple
        }
        _write_tuple(table_id, _pkey_map[table_id], key_columns);
    }

    void
    PgLogGen::stream_start()
    {
        _write_header(); // close previous msg, start new one

        if (!_in_stream_xact) {
            _add_start_xact();

            _in_stream_xact = true;
            _stream_xid = _xid++;
            _write_uint8(PgReplMsg::MSG_STREAM_START);
            _write_uint32(_stream_xid);
            _write_uint8(1);  // first stream segment
        } else {
            _write_uint8(PgReplMsg::MSG_STREAM_START);
            _write_uint32(_stream_xid);
            _write_uint8(0);  // subsequent stream segment
        }

        _write_header(); // stream ops are in their own message

        _is_streaming = true;
    }

    void
    PgLogGen::stream_stop()
    {
        _write_header(); // close previous msg, start new one

        assert(_is_streaming);
        _write_uint8(PgReplMsg::MSG_STREAM_STOP);

        _write_header(); // stream ops are in their own message

        _is_streaming = false;
    }

    void
    PgLogGen::stream_commit()
    {
        _write_header(); // close previous msg, start new one

        _add_end_xact();

        assert(_in_stream_xact);
        assert(!_is_streaming);
        uint64_t commit_lsn = _lsn;
        _write_uint8(PgReplMsg::MSG_STREAM_COMMIT);
        _write_uint32(_stream_xid);
        _write_uint8(0); // unused flags
        _write_uint64(_begin_lsn); // consistent with commit
        _write_uint64(commit_lsn);
        _write_uint64(get_pgtime_in_millis());

        std::cout << "stream commit end offset=" << ::ftell(_fp) << std::endl;

        _write_header(); // stream ops are in their own message

        _in_stream_xact = false;
    }

    void
    PgLogGen::stream_abort()
    {
        _write_header(); // close previous msg, start new one

        assert(!_in_stream_xact);
        assert(_is_streaming);
        _write_uint8(PgReplMsg::MSG_STREAM_ABORT);
        _write_uint32(_stream_xid);
        _write_uint32(_stream_xid); // sub-transaction

        _write_header(); // stream ops are in their own message

        _in_stream_xact = false;
    }

    void
    PgLogGen::dump_file(const std::filesystem::path &file_name)
    {
        FILE *fp = ::fopen(file_name.c_str(), "r");
        if (fp == nullptr) {
            throw Error("Failed to open file: " + file_name.string());
        }

        PgReplMsg pg_msg(2);

        // Read in the file from *fp until eof
        char buffer[PgLogWriter::PG_LOG_HDR_BYTES];
        while (::fread(buffer, PgLogWriter::PG_LOG_HDR_BYTES, 1, fp) > 0) {
            // decode header
            PgLogWriter::PgLogHeader header(buffer);
            assert(header.magic == PgLogWriter::PG_LOG_MAGIC);

            std::cout << header.to_string() << std::endl;

            // read in the message
            char *msg = (char *)malloc(header.msg_length);
            ::fread(msg, header.msg_length, 1, fp);

            // process the message
            pg_msg.set_buffer(msg, header.msg_length);
            while (pg_msg.has_next_msg()) {
                PgReplMsgDecoded decoded_msg = pg_msg.decode_next_msg();
                std::cout << pg_msg.dump_msg(decoded_msg) << std::endl;
            }

            free(msg);
        }
    }

    ///// PgLogGenJson /////

    void
    PgLogGenJson::parse_commands(const std::filesystem::path &file_name)
    {
        FILE *fp = ::fopen(file_name.c_str(), "r");
        if (fp == nullptr) {
            throw Error("Failed to open file: " + file_name.string());
        }
        // Read in the file from *fp until eof
        char *line = nullptr;
        size_t len = 0;
        while (::getline(&line, &len, fp) > 0) {
            if (len == 0) {
                continue;
            }

            if (line[0] == '{') {
                // Convert each line to a json object using the nlohmann::json library
                nlohmann::json json = nlohmann::json::parse(line);
                _parse_command(json);
            }
            free(line);
            line = nullptr;
        }

        ::fclose(fp);
    }

    void
    PgLogGenJson::_parse_command(const nlohmann::json &json)
    {
        std::string cmd = json["cmd"];
        if (cmd == PG_OP_BEGIN) {
            _has_begin = true;
            _parse_begin(json);
            return;
        }
        if (cmd == PG_OP_STREAM_START) {
            _has_begin = true;
            _parse_stream_start(json);
            return;
        }

        if (!_has_begin) {
            throw Error("No begin or stream start command found: cmd=" + cmd);
        }

        if (cmd == PG_OP_COMMIT) {
            _parse_commit(json);
            _has_begin = false;
            return;
        }
        if (cmd == PG_OP_CREATE_TABLE) {
            _parse_create_table(json);
            return;
        }
        if (cmd == PG_OP_ALTER_TABLE) {
            _parse_alter_table(json);
            return;
        }
        if (cmd == PG_OP_DROP_TABLE) {
            _parse_drop_table(json);
            return;
        }
        if (cmd == PG_OP_TRUNCATE) {
            _parse_truncate(json);
            return;
        }
        if (cmd == PG_OP_INSERT) {
            _parse_insert(json);
            return;
        }
        if (cmd == PG_OP_UPDATE) {
            _parse_update(json);
            return;
        }
        if (cmd == PG_OP_DELETE) {
            _parse_delete(json);
            return;
        }
        if (cmd == PG_OP_STREAM_STOP) {
            _parse_stream_stop(json);
            return;
        }
        if (cmd == PG_OP_STREAM_COMMIT) {
            _parse_stream_commit(json);
            return;
        }
        if (cmd == PG_OP_STREAM_ABORT) {
            _parse_stream_abort(json);
            return;
        }


        throw Error("Unknown command: " + cmd);
    }

    std::vector<PgMsgSchemaColumn>
    PgLogGenJson::_parse_columns(const nlohmann::json &json)
    {
        std::vector<PgMsgSchemaColumn> columns;
        int i = 0;
        for (auto &&c: json["columns"]) {
            PgMsgSchemaColumn col;
            col.column_name = c["name"];
            col.is_nullable = c["is_nullable"];
            col.udt_type = c["type"];
            if (!c.contains("default") || c["default"].is_null()) {
                col.default_value = std::nullopt;
            } else {
                col.default_value = c["default"];
            }
            col.is_pkey = c["is_pkey"];
            col.position = i++;
            columns.push_back(col);
        }
        return columns;
    }

    uint32_t
    PgLogGenJson::_get_table_id(const std::string &table_name)
    {
        auto &&it = _table_name_to_id.find(table_name);
        if (it == _table_name_to_id.end()) {
            throw Error("Table not found: " + table_name);
        }
        return it->second;
    }

    void
    PgLogGenJson::_parse_create_table(const nlohmann::json &json)
    {
        std::string table = json["table"];

        std::vector<PgMsgSchemaColumn> columns = _parse_columns(json);
        uint32_t table_id = _log_gen.create_table(table, columns);

        _table_name_to_id[table] = table_id;
    }

    void
    PgLogGenJson::_parse_alter_table(const nlohmann::json &json) {
        std::string table = json["table"];

        std::vector<PgMsgSchemaColumn> columns = _parse_columns(json);
        uint32_t table_id = _get_table_id(table);
        _log_gen.alter_table(table_id, columns);
    }

    void
    PgLogGenJson::_parse_drop_table(const nlohmann::json &json)
    {
        std::string table = json["table"];
        uint32_t table_id = _get_table_id(table);
        _log_gen.drop_table(table_id);
    }

    void
    PgLogGenJson::_parse_begin(const nlohmann::json &json)
    {
        _log_gen.begin();
    }

    void
    PgLogGenJson::_parse_commit(const nlohmann::json &json)
    {
        _log_gen.commit();
    }

    void
    PgLogGenJson::_parse_truncate(const nlohmann::json &json)
    {
        std::string table = json["table"];
        uint32_t table_id = _table_name_to_id[table];
        _log_gen.truncate(table_id);
    }

    void
    PgLogGenJson::_parse_insert(const nlohmann::json &json)
    {
        std::string table = json["table"];
        uint32_t table_id = _get_table_id(table);
        std::vector<std::string> row_columns;
        for (auto &&c: json["row"]) {
            if (c.is_null()) {
                row_columns.push_back(PgLogGen::PG_VALUE_NULL);
            } else if (c.is_boolean()) {
                if (c.get<bool>()) {
                    row_columns.push_back(PgLogGen::PG_VALUE_TRUE);
                } else {
                    row_columns.push_back(PgLogGen::PG_VALUE_FALSE);
                }
            } else {
                row_columns.push_back(c);
            }
        }
        _log_gen.insert(table_id, row_columns);
    }

    void
    PgLogGenJson::_parse_update(const nlohmann::json &json)
    {
        std::string table = json["table"];
        uint32_t table_id = _get_table_id(table);
        std::vector<std::string> key_columns;
        for (auto &&c: json["key"]) {
            key_columns.push_back(c);
        }
        std::vector<std::string> row_columns;
        for (auto &&c: json["row"]) {
            row_columns.push_back(c);
        }
        bool is_pkey = json["is_pkey"];
        _log_gen.update(table_id, key_columns, row_columns, is_pkey);
    }

    void
    PgLogGenJson::_parse_delete(const nlohmann::json &json)
    {
        std::string table = json["table"];
        uint32_t table_id = _get_table_id(table);
        std::vector<std::string> key_columns;
        for (auto &&c: json["key"]) {
            key_columns.push_back(c);
        }
        bool is_pkey = json["is_pkey"];
        _log_gen.delrow(table_id, key_columns, is_pkey);
    }

    void
    PgLogGenJson::_parse_stream_start(const nlohmann::json &json)
    {
        _log_gen.stream_start();
    }

    void
    PgLogGenJson::_parse_stream_stop(const nlohmann::json &json)
    {
        _log_gen.stream_stop();
    }

    void
    PgLogGenJson::_parse_stream_commit(const nlohmann::json &json)
    {
        _log_gen.stream_commit();
    }

    void
    PgLogGenJson::_parse_stream_abort(const nlohmann::json &json)
    {
        _log_gen.stream_abort();
    }


} // namespace springtail

