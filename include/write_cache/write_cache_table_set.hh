#pragma once

#include <common/timestamp.hh>
#include <write_cache/write_cache_index_common.hh>
#include <write_cache/write_cache_index_node.hh>

namespace springtail {

    /** Encapsulation of an index for a set of table partitions */
    class WriteCacheTableSet {
    public:
        struct Metadata
        {
            PostgresTimestamp pg_commit_ts; // postgres-reported commit ts of xid
            std::chrono::steady_clock::time_point local_begin_ts; //local begin transaction ts
            std::chrono::steady_clock::time_point local_commit_ts; //local commit ts
        };

        /**
         * @brief Construct a new Write Cache Table Set object
         */
        explicit WriteCacheTableSet(const std::filesystem::path &db_dir_path);

        /**
         * @brief Add extent to table set
         * @param tid table ID
         * @param pg_xid Postgres XID
         * @param lsn LSN
         * @param data extent data
         */
        void add_extent(uint64_t tid, uint64_t pg_xid, uint64_t lsn, const ExtentPtr data, bool on_disk);

        /**
         * @brief Drop table from set
         * @param tid table ID
         * @param pg_xid Postgres XID
         */
        void drop_table(uint64_t tid, uint64_t pg_xid, uint64_t &memory_removed);

        /**
         * @brief Abort a transaction, remove data for pg xid
         * @param pg_xid
         */
        void abort(uint64_t pg_xid, uint64_t &memory_removed);

        /**
         * @brief Add mapping from springtail XID to Postgres XID
         * @param pg_xid Postgres XID
         * @param xid springtail XID
         * @param md metadata
         */
        void commit(uint64_t pg_xid, uint64_t xid, Metadata md);

        /**
         * @brief Add mapping from springtail XID to Postgres XID
         * @param pg_xids Postgres XID
         * @param xid springtail XID
         * @param commit_ts postgres commit ts
         */
        void commit(const std::vector<uint64_t>& pg_xids, uint64_t xid, Metadata md);

        /**
         * @brief Get a list of table IDs
         * @param xid springtail XID
         * @param count number of items to return; may be less
         * @param start_offset offset at which to start searching, may be larger then partitions set
         * @param cursor out; set to the offset of the last table returned
         * @param result reference to result vector (thrift only supports int64, so that is what we use)
         * @return int number of elements added
         */
        int get_tids(uint64_t xid, uint32_t count, uint64_t start_offset,
                     uint64_t &cursor, std::vector<uint64_t> &result);

        /**
         * @brief Get a list of extents for a table at a given XID
         * @param tid table ID
         * @param xid springtail XID
         * @param count number of items to return
         * @param start_offset offset at which to start searching
         * @param cursor out; set to the offset of the last extent returned
         * @param md out; metadata
         * @param result reference to result vector
         * @return int number of elements added
         */
        int get_extents(uint64_t tid, uint64_t xid, uint32_t count,
                        uint64_t start_offset, uint64_t &cursor,
                        std::vector<WriteCacheIndexExtentPtr> &result, Metadata &md);

        /**
         * @brief Evict all data for table, fixup indexes
         * @param tid table ID
         * @param xid springtail XID
         * @param memory_removed amount of memory freed by this function
         */
        void evict_table(uint64_t tid, uint64_t xid, uint64_t &memory_removed);

        /**
         * @brief Evict all data for XID, fixup indexes
         * @param xid
         * @param memory_removed amount of memory freed by this function
         * @param pg_xids_removed list of removed pg_xids
         */
        void evict_xid(uint64_t xid, uint64_t &memory_removed, std::set<uint64_t> &pg_xids_removed);

        /**
         * @brief Helper utility to dump from _table_root
         */
        void dump();

        /**
         * @brief Get the json stats object
         *
         * @return nlohmann::json
         */
        nlohmann::json get_stats();

    private:
        /** The name of the path for storing extents on disk */
        std::filesystem::path _db_dir_path;

        /** root of tree, each level points to another set of ids sorted by max xid */
        WriteCacheIndexNodePtr _xid_root;

        /** map of sp xid to pg_xids (there may be multiple pg_xids due to subtransactions) */
        std::unordered_multimap<uint64_t, uint64_t> _xid_map;

        /** map of sp xid to Postgres commit ts */
        std::unordered_map<uint64_t, Metadata> _xid_ts_map;

        /** mutex for _xid_map and _xid_ts_map */
        std::shared_mutex _xid_map_mutex;

        /**
         * @brief Add extent on disk to table set
         *
         * @param tid table ID
         * @param pg_xid Postgres XID
         * @param lsn LSN
         * @param extent_offset extent offset
         * @param extent_size extent size
         */
        void _add_extent_on_disk(uint64_t pg_xid, ExtentPtr data, uint64_t &extent_offset, size_t &extent_size);

        /**
         * @brief Utility helper to dump tree from provided root
         * @param node Root of tree to dump (called from public dump())
         */
        void _dump(WriteCacheIndexNodePtr node);

        /**
         * @brief Get pg_xids that map to a springtail XID
         * @param xid Springtail XID
         * @param md out; metadata
         * @return std::set<uint64_t> of PG XIDs
         */
        std::set<uint64_t> _lookup_pgxid(uint64_t xid, Metadata *md=nullptr);
    };
    typedef std::shared_ptr<WriteCacheTableSet> WriteCacheTableSetPtr;
}
