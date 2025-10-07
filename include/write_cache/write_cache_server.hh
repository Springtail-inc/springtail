#pragma once

#include <boost/thread/shared_mutex.hpp>
#include <grpc/grpc_server_manager.hh>

#include <common/init.hh>
#include <write_cache/write_cache_index.hh>

namespace springtail {

    class WriteCacheServer final : public Singleton<WriteCacheServer>
    {
        friend class Singleton<WriteCacheServer>;
    public:

        /**
         * @brief Add an extent to the write cache
         * @param db_id database ID
         * @param tid table ID
         * @param pg_xid Postgres XID
         * @param lsn LSN of the extent
         * @param data extent data
         */
        void
        add_extent(uint64_t db_id, uint64_t tid, uint64_t pg_xid, uint64_t lsn, const ExtentPtr data);

        /**
         * @brief Drop a table from the write cache and all of its data
         * @param db_id database ID
         * @param tid table ID
         * @param pg_xid Postgres XID
         */
        void
        drop_table(uint64_t db_id, uint64_t tid, uint64_t pg_xid);

        /**
         * @brief Commit a transaction, add mapping from springtail XID to Postgres XID
         * @param db_id database ID
         * @param xid Springtail XID
         * @param pg_xid Postgres XIDs
         */
        void
        commit(uint64_t db_id, uint64_t xid, const std::vector<uint64_t>& pg_xids, WriteCacheTableSet::Metadata md);

        /**
         * @brief Commit a transaction, add mapping from springtail XID to Postgres XID
         * @param db_id database ID
         * @param xid Springtail XID
         * @param pg_xid Postgres XIDs
         * @param md Metadata
         */
        void
        commit(uint64_t db_id, uint64_t xid, uint64_t pg_xid, WriteCacheTableSet::Metadata md);

        /**
         * @brief Abort a transaction, remove all data for a given Postgres XID
         * @param db_id database ID
         * @param pg_xid Postgres XID
         */
        void
        abort(uint64_t db_id, uint64_t pg_xid);

        /**
         * @brief Abort a transaction, remove all data for a given Postgres XID
         * @param db_id database ID
         * @param pg_xids Postgres XID
         */
        void
        abort(uint64_t db_id, const std::vector<uint64_t> &pg_xids);

        /**
         * @brief List tables for a given XID
         * @param db_id database ID
         * @param xid Springtail XID
         * @param count number of tables to return
         * @param cursor cursor for pagination
         * @return vector of table IDs
         */
        std::vector<uint64_t>
        list_tables(uint64_t db_id, uint64_t xid, uint32_t count, uint64_t& cursor);

        /**
         * @brief Get extents for a given XID
         * @param db_id database ID
         * @param tid table ID
         * @param xid Springtail XID
         * @param count number of extents to return
         * @param cursor cursor for pagination
         * @param commit_ts out; postgres-reported commit ts of xid
         * @return vector of extents
         */
        std::vector<WriteCacheIndexExtentPtr>
        get_extents(uint64_t db_id, uint64_t tid, uint64_t xid, uint32_t count, uint64_t &cursor, WriteCacheTableSet::Metadata &md);

        /**
         * @brief Evict given XID from the write cache
         * @param db_id database ID
         * @param xid Springtail XID
         */
        void
        evict_xid(uint64_t db_id, uint64_t xid);

        void
        evict_table(uint64_t db_id, uint64_t tid, uint64_t xid);

        void
        drop_database(uint64_t db_id);

        nlohmann::json
        get_memory_stats();

        /**
         * @brief Get the write cache index object
         * @return std::shared_ptr<WriteCacheIndex>
         */
        std::shared_ptr<WriteCacheIndex>
        get_index(uint64_t db_id);


    private:
        /**
         * @brief Construct a new Write Cache Server object
         *
         */
        WriteCacheServer();
        virtual ~WriteCacheServer() override = default;

        /**
         * @brief Function for starting write cache server
         *
         */
        void _startup();

        /**
         * @brief Subtract the given memory size from current memory variable and adjust
         *  store to disk flag appropriately.
         *
         * @param mem_size
         */
        void
        _subtract_memory(uint64_t mem_size);

        /**
         * @brief Add the given memory size to the current memory variable and adjust
         *  store to disk flag appropriately.
         *
         * @param mem_size
         */
        void
        _add_memory(uint64_t mem_size);

        /** indexes mutex */
        boost::shared_mutex _db_mutex;

        /** map of indexes by db_id */
        std::map<uint64_t, WriteCacheIndexPtr> _indexes;

        /** top level path for storing extents on disk */
        std::filesystem::path _disk_storage_dir;

        std::shared_mutex _mem_mutex;                 ///< memory data mutex
        uint64_t _memory_high_watermark_bytes;        ///< high watermark level
        uint64_t _memory_low_watermark_bytes;         ///< low watermark level
        uint64_t _current_memory_bytes{0};            ///< current memory size used by in-memory extents
        std::atomic<bool> _store_to_disk{false};      ///< flag indicating storage on disk

        GrpcServerManager _grpc_server_manager;       ///< GRPC server

        /**
         * @brief Internal shutdown function called by Singleton
         *
         */
        void _internal_shutdown() override;
    };

} // namespace springtail
