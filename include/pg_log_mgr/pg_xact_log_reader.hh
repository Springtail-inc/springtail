#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <fstream>

#include <pg_repl/pg_repl_msg.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Postgres transaction log reader
     */
    class PgXactLogReader {
    public:
        /**
         * @brief Construct a new Pg Xact Log Reader object
         * @param base_dir    base directory for log files
         * @param file_prefix file prefix
         * @param file_suffix file suffix
         */
        PgXactLogReader(const std::filesystem::path &base_dir,
                        const std::string &file_prefix,
                        const std::string &file_suffix)
            : _base_dir(base_dir), _file_prefix(file_prefix), _file_suffix(file_suffix) {}

        /**
         * @brief Scan all log files, extract committed xacts, and in progress stream xacts
         * @param committed_xid last committed xid
         */
        void scan_all_files(uint64_t committed_xid);

        /**
         * @brief Scan single log file, extract committed xacts, and in progress stream xacts
         * @param file file path
         * @param committed_xid last committed xid
         */
        void scan_file(const std::filesystem::path &file, uint64_t committed_xid);

        /**
         * @brief Get the xact list object
         * @return std::vector<PgTransactionPtr>
         */
        std::vector<PgTransactionPtr> get_xact_list() {
            return _xact_list;
        }

        /**
         * @brief Get the stream map object
         * @return std::map<uint32_t, PgTransactionPtr>
         */
        std::map<uint32_t, PgTransactionPtr> get_stream_map() {
            return _stream_map;
        }

        /**
         * @brief Get the max springtail xid in log file
         * @return uint64_t max springtail xid
         */
        uint64_t get_max_sp_xid() const {
            return _max_sp_xid;
        }

    private:
        std::filesystem::path _base_dir;  ///< base directory

        std::string _file_prefix;  ///< file prefix
        std::string _file_suffix;  ///< file suffix

        std::fstream _stream;      ///< file stream

        uint64_t _max_sp_xid=0;    ///< max springtail xid in log file

        std::vector<PgTransactionPtr> _xact_list;  ///< list of xacts in log files > committed xid
        std::map<uint32_t, PgTransactionPtr> _stream_map;  ///< map of in progress stream xacts

        /**
         * @brief Read the next committed xact from file
         * @param msg_len  message length
         * @param xid      springtail xid (last committed xid) used as filter
         */
        void _read_commit(uint32_t msg_len, uint64_t committed_xid);

        /**
         * @brief Read the next stream start from file
         * @param msg_len message length
         * @param type    message type
         */
        void _read_stream_msg(uint32_t msg_len, uint8_t type);

        /** Open file */
        void _open(const std::filesystem::path &path);
    };
} // namespace springtail::pg_log_mgr