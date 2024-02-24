#include <stdio.h>

#include <vector>
#include <map>
#include <filesystem>
#include <string>

#include <nlohmann/json.hpp>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_msg.hh>

namespace springtail {
    /**
     * @brief Generate a postgres log file based on operations
     */
    class PgLogGen {
    public:
        static constexpr char PG_SCHEMA_TYPE_INT4[] = "int4"; ///< int4 schema type
        static constexpr char PG_SCHEMA_TYPE_INT8[] = "int8"; ///< int8 schema type
        static constexpr char PG_SCHEMA_TYPE_TEXT[] = "text"; ///< text schema type
        static constexpr char PG_SCHEMA_TYPE_BOOL[] = "bool"; ///< bool schema type

        static constexpr char PG_VALUE_NULL[] = "\\N";    ///< null value
        static constexpr char PG_VALUE_TRUE[] = "true";   ///< true value
        static constexpr char PG_VALUE_FALSE[] = "false"; ///< false value

        static constexpr int PG_WRAP_LSN_BYTES = 128;     ///< wrap lsn after this many bytes

        /**
         * @brief Construct a new Pg Log Gen object
         * @param file_name output filename
         */
        PgLogGen(const std::filesystem::path &file_name);

        /** Destructor */
        ~PgLogGen();

        /**
         * @brief Create a table
         * @param table_name table name
         * @param columns list of columns, and types
         * @return uint32_t table ID
         */
        uint32_t create_table(const std::string &table_name, const std::vector<PgMsgSchemaColumn> &columns);

        /**
         * @brief Alter table
         * @param table_id table id
         * @param columns list of columns, and types
         */
        void alter_table(uint32_t table_id, const std::vector<PgMsgSchemaColumn> &columns);

        /**
         * @brief Drop a table
         * @param table_id table ID
         */
        void drop_table(uint32_t table_id);

        /**
         * @brief Start a transaction, every begin must end with a commit
         * @return uint32_t transaction ID
         */
        uint32_t begin();

        /**
         * @brief Commit a transaction
         */
        void commit();

        /**
         * @brief Insert a row into a table
         * @param table_id table ID
         * @param row_columns list of column values
         */
        void insert(uint32_t table_id, const std::vector<std::string> &row_columns);

        /**
         * @brief Update a row in a table
         * @param table_id table ID
         * @param key_columns list of key columns
         * @param row_columns list of column values
         * @param using_pkey true if using primary key, false if using all columns
         */
        void update(uint32_t table_id, const std::vector<std::string> &key_columns,
                    const std::vector<std::string> &row_columns, bool using_pkey = true);

        /**
         * @brief Delete a row from a table
         * @param table_id table ID
         * @param key_columns list of key columns
         * @param using_pkey true if using primary key, false if using all columns
         */
        void delrow(uint32_t table_id, const std::vector<std::string> &key_columns, bool using_pkey = true);

        /**
         * @brief Truncate a table
         * @param table_id table ID
         */
        void truncate(uint32_t table_id);

        /** Start of stream */
        void stream_start();

        /** End of stream */
        void stream_stop();

        /** Commit stream */
        void stream_commit();

        /** Abort stream */
        void stream_abort();

        /** Get list of transaction start/end pairs */
        std::vector<PgReplMsgStream::PgTransactionPtr> get_xact_list() { return _xact_list; }

        /**
         * @brief Dump log file
         * @param file_name file to dump
         */
        static void dump_file(const std::filesystem::path &file_name);

    private:
        std::filesystem::path _file_name; ///< file name
        FILE *_fp;                        ///< file pointer
        int _bytes_written = 0;           ///< bytes written, reset after PG_WRAP_LSN_BYTES
        int _header_offset = 0;           ///< header offset; where to write header of current message
        uint64_t _begin_lsn = 0;          ///< begin lsn

        uint32_t _next_table_id = 0;      ///< next table id
        uint32_t _xid = 0;                ///< transaction id
        uint64_t _lsn = 0;                ///< lsn (incremented for every PG_WRAP_LSN_BYTES written)
        uint64_t _commit_ts = 0;          ///< commit timestamp

        uint32_t _stream_xid = -1;        ///< current stream transaction id if in stream xact
        bool _in_stream_xact = false;     ///< true if in streaming transaction
        bool _is_streaming = false;       ///< true if streaming

        PgReplMsgStream::PgTransactionPtr _current_xact; ///< current transaction
        std::vector<PgReplMsgStream::PgTransactionPtr> _xact_list; ///< transaction list

        std::map<uint32_t, std::string> _table_id_to_name; ///< table id to name
        std::map<uint32_t, std::vector<std::string>> _schema_map; ///< table id to schema type list
        std::map<uint32_t, std::vector<std::string>> _pkey_map; ///< table id to primary key type list

        /** write uint64 */
        void _write_uint64(uint64_t val) {
            char buffer[8];
            sendint64(val, buffer);
            _write(buffer, 8);
        }

        /** write uint32 */
        void _write_uint32(uint64_t val) {
            char buffer[4];
            sendint32(val, buffer);
            _write(buffer, 4);
        }

        /** write uint16 */
        void _write_uint16(uint64_t val) {
            char buffer[2];
            sendint16(val, buffer);
            _write(buffer, 2);
        }

        /** write uint8/char/bool */
        void _write_uint8(uint8_t val) {
            _write((const char *)&val, 1);
        }

        /**
         * @brief Generate table message json data (for alter table or create table)
         */
        nlohmann::json _gen_table_schema(uint32_t table_id, const std::vector<PgMsgSchemaColumn> &columns);

        /** write data */
        void _write(const char *data, size_t size);

        /** write string, null terminated */
        void _write_string(const std::string &str);

        /** write json message */
        void _write_message(const char *prefix, const nlohmann::json &msg);

        /** write tuple data */
        void _write_tuple(uint32_t table_id, const std::vector<std::string> &types, const std::vector<std::string> &columns);

        /** write header */
        void _write_header();

        /** create _current_xact and populate with start of xact data */
        void _add_start_xact();

        /** complete _current_xact with end of xact data, and add to xact list */
        void _add_end_xact();
    };

    /**
     * @brief Write a postgres log file based on commands in json format.  One command per line.
     */
    class PgLogGenJson {
    public:
        static constexpr char PG_OP_INSERT[] = "insert";
        static constexpr char PG_OP_DELETE[] = "delete";
        static constexpr char PG_OP_UPDATE[] = "update";
        static constexpr char PG_OP_TRUNCATE[] = "truncate";
        static constexpr char PG_OP_ALTER_TABLE[] = "alter table";
        static constexpr char PG_OP_CREATE_TABLE[] = "create table";
        static constexpr char PG_OP_DROP_TABLE[] = "drop table";
        static constexpr char PG_OP_BEGIN[] = "begin";
        static constexpr char PG_OP_COMMIT[] = "commit";
        static constexpr char PG_OP_STREAM_START[] = "stream start";
        static constexpr char PG_OP_STREAM_STOP[] = "stream stop";
        static constexpr char PG_OP_STREAM_COMMIT[] = "stream commit";
        static constexpr char PG_OP_STREAM_ABORT[] = "stream abort";

        /**
         * @brief Construct a new Pg Log Gen Json object
         * @param outfile output file
         */
        PgLogGenJson(const std::filesystem::path &outfile) : _log_gen(outfile) {}

        /**
         * @brief Parse commands from file; one command per line in JSON
         * @param file_name file name
         */
        void parse_commands(const std::filesystem::path &file_name);

        /**
         * @brief Get the xact list from PgLogGen object
         * @return std::vector<PgReplMsgStream::PgTransactionPtr>
         */
        std::vector<PgReplMsgStream::PgTransactionPtr> get_xact_list() { return _log_gen.get_xact_list(); }

    private:
        PgLogGen _log_gen; ///< log generator

        bool _has_begin = false; ///< true if begin has been called

        std::map<std::string, uint32_t> _table_name_to_id; ///< table name to id

        void _parse_command(const nlohmann::json &cmd); ///< parse json command

        /** Convert json columns to list of pg msg columns */
        std::vector<PgMsgSchemaColumn> _parse_columns(const nlohmann::json &json);

        /** Get table id from name */
        uint32_t _get_table_id(const std::string &table_name);

        /** Parse create table op -- calls into _log_gen */
        void _parse_create_table(const nlohmann::json &json);

        /** Parse alter table op -- calls into _log_gen */
        void _parse_alter_table(const nlohmann::json &json);

        /** Parse drop table op -- calls into _log_gen */
        void _parse_drop_table(const nlohmann::json &json);

        /** Parse begin op -- calls into _log_gen */
        void _parse_begin(const nlohmann::json &json);

        /** Parse commit op -- calls into _log_gen */
        void _parse_commit(const nlohmann::json &json);

        /** Parse truncate op -- calls into _log_gen */
        void _parse_truncate(const nlohmann::json &json);

        /** Parse insert op -- calls into _log_gen */
        void _parse_insert(const nlohmann::json &json);

        /** Parse update op -- calls into _log_gen */
        void _parse_update(const nlohmann::json &json);

        /** Parse delete op -- calls into _log_gen */
        void _parse_delete(const nlohmann::json &json);

        /** Parse stream start op -- calls into _log_gen */
        void _parse_stream_start(const nlohmann::json &json);

        /** Parse stream stop op -- calls into _log_gen */
        void _parse_stream_stop(const nlohmann::json &json);

        /** Parse stream commit op -- calls into _log_gen */
        void _parse_stream_commit(const nlohmann::json &json);

        /** Parse stream abort op -- calls into _log_gen */
        void _parse_stream_abort(const nlohmann::json &json);
    };
}