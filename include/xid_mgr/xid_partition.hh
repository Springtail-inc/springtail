#pragma once

#include <fcntl.h>
#include <sys/stat.h>

#include <mutex>
#include <shared_mutex>
#include <memory>
#include <vector>
#include <string>
#include <map>
#include <filesystem>
#include <thread>
#include <atomic>
#include <condition_variable>

namespace springtail::xid_mgr {
    class Partition {

    public:
        /** fsync sleep time in seconds */
        static constexpr int const SYNC_SLEEP_TIME_SECS = 3;

        /** prefix for partition files */
        static constexpr char const PARTITION_FILE_PREFIX[] = "partition_";

        /** number of <db_id, xid> pairs to store */
        static constexpr int const MAX_ENTRIES = 512;

        /** buffer size for writing to file based on 16B per entry */
        static constexpr int const BUFFER_SIZE = 16 * MAX_ENTRIES;

        /** magic header for file */
        static constexpr uint64_t const MAGIC_HDR = 0xDEADBEEFABBADEAF;

        /** Constructor */
        Partition(const std::filesystem::path &base_path, int id);

        /** Destructor */
        ~Partition();

        /**
         * @brief commit up to and including given xid
         * @param db_id database id
         * @param xid xid to commit
         */
        void commit_xid(uint64_t db_id, uint64_t xid, bool has_schema_changes);

        /**
         * @brief Record a DDL change without doing a commit.  Used for table sync operations.
         * @param db_id database id
         * @param xid xid to commit
         */
        void record_ddl_change(uint64_t db_id, uint64_t xid);

        /**
         * @brief Get the latest committed xid object
         * @param db_id database id
         * @param schema_xid last known schema xid
         * @return uint64_t
         */
        uint64_t get_committed_xid(uint64_t db_id, uint64_t schema_xid);

        /**
         * @brief Load partition from file
         */
        void load()
        {
            _read_committed_xids();
        }

        /**
         * @brief Get db ids
         * @return vector of db ids
         */
        std::vector<uint64_t> get_db_ids()
        {
            std::shared_lock lock(_map_mutex);
            // copy db_ids from _committed_xids to vector
            std::vector<uint64_t> db_ids;
            for (auto &it : _committed_xids) {
                db_ids.push_back(it.first);
            }
            return db_ids;
        }

        void shutdown() {
            std::unique_lock lock(_shutdown_mutex);
            _shutdown = true;
            _shutdown_cv.notify_all();
            lock.unlock();
            _sync_thread.join();
        }

    private:
        int _id; ///< partition id
        int _fd; ///< file descriptor

        std::filesystem::path _path;     ///< base path

        std::atomic<bool> _dirty{false};        ///< dirty flag
        std::atomic<bool> _shutdown{false};     ///< shutdown flag

        std::mutex _file_mutex;          ///< mutex for file operations
        std::mutex _shutdown_mutex;      ///< mutex for shutdown
        std::condition_variable _shutdown_cv; ///< condition variable for shutdown

        std::shared_mutex _map_mutex;    ///< mutex for updating the maps

        /** last committed xid by db_id */
        std::map<uint64_t, uint64_t> _committed_xids;

        /** history of schema xids by db_id */
        std::map<uint64_t, std::vector<uint64_t>> _history;

        std::thread _sync_thread;        ///< thread for syncing to disk

        /**
         * @brief Write committed xid to file (if larger than last value)
         */
        void _write_committed_xids();

        /**
         * @brief Read committed xids from file
         */
        void _read_committed_xids();

        /**
         * @brief Sync committed xids to disk
         */
        void _sync_thread_func();

        /**
         * @brief Add an entry to the DDL history
         * @param db_id database id
         * @param xid xid to write
         */
        void _add_history(uint64_t db_id, uint64_t xid);
    };
    using PartitionPtr = std::shared_ptr<Partition>;
}
