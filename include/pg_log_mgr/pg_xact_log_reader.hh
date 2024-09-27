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
                        const std::string &file_suffix,
                        uint64_t min_xid=0)
            : _base_dir(base_dir), _file_prefix(file_prefix),
              _file_suffix(file_suffix), _min_xid(min_xid) {}

        PgXactLogReader(const std::string &file_prefix,
                        const std::string &file_suffix,
                        uint64_t min_xid=0)
            : _base_dir("/tmp/springtail"), _file_prefix(file_prefix),
              _file_suffix(file_suffix), _min_xid(min_xid) {}

        /** Destructor, close stream */
        ~PgXactLogReader() {
            if (_stream.is_open()) {
                try { _stream.close(); } catch(...) {}
            }
        }

        /**
         * @brief Scan all log files, extract committed xacts, and in progress stream xacts
         */
        void begin();

        /**
         * @brief Scan single log file, extract committed xacts, and in progress stream xacts
         * @param file file path
         */
        void begin(const std::filesystem::path &file);

        /**
         * @brief Scan all log files, extract committed xacts
         * @param max_records max records to return
         * @param committed_xacts committed xacts output vector
         */
        int next(int max_records, std::vector<PgTransactionPtr> &committed_xacts);

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
        std::string _file_prefix;         ///< file prefix
        std::string _file_suffix;         ///< file suffix

        std::filesystem::path _current_file;  ///< current file path
        std::fstream _stream;                 ///< file stream
        uint64_t _max_sp_xid=0;               ///< max springtail xid in log file

        std::map<uint32_t, PgTransactionPtr> _stream_map;  ///< map of in progress stream xacts

        uint64_t _min_xid = 0;                        ///< min xid to read

        /**
         * @brief Read the next committed xact from file
         * @param msg_len  message length
         * @param xid      springtail xid (last committed xid) used as filter
         * @return PgTransactionPtr (or nullptr if message is being skipped)
         */
        PgTransactionPtr _read_commit(uint32_t msg_len, uint64_t committed_xid);

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