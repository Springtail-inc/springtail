#pragma once

#include <common/timestamp.hh>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_server.hh>
#include <storage/extent.hh>

namespace springtail {
    /**
     * @brief Functional implementation of the write cache service
     * Predominantly uses pg_xid and not springtail XIDs.  Used by
     * the log mgr and parser.
     */
    class WriteCacheFuncImpl {
    public:

        /** Delete constructor */
        WriteCacheFuncImpl() = delete;

        /** Delete copy constructor */
        WriteCacheFuncImpl(const WriteCacheFuncImpl &) = delete;
        WriteCacheFuncImpl &operator=(const WriteCacheFuncImpl &) = delete;

        /**
         * @brief Add an extent to the write cache
         * @param db_id database ID
         * @param tid table ID
         * @param pg_xid Postgres XID
         * @param lsn LSN of the extent
         * @param data extent data
         */
        static void add_extent(uint64_t db_id, uint64_t tid, uint64_t pg_xid, uint64_t lsn, const ExtentPtr data)
        {
            WriteCacheIndexPtr index = WriteCacheServer::get_instance()->get_index(db_id);
            index->add_extent(tid, pg_xid, lsn, data);
        }

        /**
         * @brief Drop a table from the write cache and all of its data
         * @param db_id database ID
         * @param tid table ID
         * @param pg_xid Postgres XID
         */
        static void drop_table(uint64_t db_id, uint64_t tid, uint64_t pg_xid)
        {
            WriteCacheIndexPtr index = WriteCacheServer::get_instance()->get_index(db_id);
            index->drop_table(tid, pg_xid);
        }

        /**
         * @brief Commit a transaction, add mapping from springtail XID to Postgres XID
         * @param db_id database ID
         * @param xid Springtail XID
         * @param pg_xid Postgres XIDs
         */
        static void commit(uint64_t db_id, uint64_t xid, std::vector<uint64_t> pg_xids, PostgresTimestamp commit_ts)
        {
            WriteCacheIndexPtr index = WriteCacheServer::get_instance()->get_index(db_id);
            index->commit(pg_xids, xid, commit_ts);
        }

        /**
         * @brief Commit a transaction, add mapping from springtail XID to Postgres XID
         * @param db_id database ID
         * @param xid Springtail XID
         * @param pg_xid Postgres XIDs
         * @param commit_ts Postgres commit ts
         */
        static void commit(uint64_t db_id, uint64_t xid, uint64_t pg_xid, PostgresTimestamp commit_ts)
        {
            WriteCacheIndexPtr index = WriteCacheServer::get_instance()->get_index(db_id);
            index->commit(pg_xid, xid, commit_ts);
        }

        /**
         * @brief Abort a transaction, remove all data for a given Postgres XID
         * @param db_id database ID
         * @param pg_xid Postgres XID
         */
        static void abort(uint64_t db_id, uint64_t pg_xid)
        {
            WriteCacheIndexPtr index = WriteCacheServer::get_instance()->get_index(db_id);
            index->abort(pg_xid);
        }

        /**
         * @brief Abort a transaction, remove all data for a given Postgres XID
         * @param db_id database ID
         * @param pg_xids Postgres XID
         */
        static void abort(uint64_t db_id, std::vector<uint64_t> pg_xids)
        {
            WriteCacheIndexPtr index = WriteCacheServer::get_instance()->get_index(db_id);
            index->abort(pg_xids);
        }
    };
} // namespace springtail
