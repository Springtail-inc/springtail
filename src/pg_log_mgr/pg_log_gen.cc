#include <stdlib.h>
#include <stdio.h>
#include <cassert>

#include <nlohmann/json.hpp>

#include <common/common.hh>
#include <common/exception.hh>

#include <pg_log_mgr/pg_log_gen.hh>

namespace springtail {

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
                row_columns.push_back(PgMsgLogGen::PG_VALUE_NULL);
            } else if (c.is_boolean()) {
                if (c.get<bool>()) {
                    row_columns.push_back(PgMsgLogGen::PG_VALUE_TRUE);
                } else {
                    row_columns.push_back(PgMsgLogGen::PG_VALUE_FALSE);
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
