#include <stdio.h>

#include <vector>
#include <map>
#include <filesystem>
#include <string>

#include <nlohmann/json.hpp>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_log_gen.hh>

namespace springtail {
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
        std::vector<PgTransactionPtr> get_xact_list() { return _log_gen.get_xact_list(); }

    private:
        PgMsgLogGen _log_gen;    ///< log generator

        bool _has_begin = false; ///< true if begin has been called

        std::map<std::string, uint32_t> _table_name_to_id; ///< table name to id

        /** Parse json command */
        void _parse_command(const nlohmann::json &cmd);

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