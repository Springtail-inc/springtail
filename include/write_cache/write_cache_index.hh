#pragma once

#include <map>
#include <string>
#include <memory>
#include <vector>

#include <fmt/core.h>

#include <common/timestamp.hh>

#include <write_cache/write_cache_index_common.hh>
#include <write_cache/write_cache_table_set.hh>

#include <storage/extent.hh>

namespace springtail {
    /**
     * @brief Write Cache Index -- the core index interface; Contains a set of partitions based on Table ID
     * write_cache_table_set.cc implements the logic within a partition -- which is the core impl of the index.
     */
    class WriteCacheIndex {
    public:
        /**
         * @brief Construct a new Write Cache Index object with a set of partitions
         *        Each partition holds an index for a set of tables (table_id is hashed to determine partition)
         * @param partitions
         */
        WriteCacheIndex(int partitions=8) : _num_partitions(partitions)
        {
            for (int i=0; i < partitions; i++) {
                _partitions.push_back(std::make_shared<WriteCacheTableSet>());
            }
        }

        /**
         * @brief Add a new extent to the index
         * @param tid table ID
         * @param pg_xid Postgres XID
         * @param lsn LSN of the extent
         * @param data extent data
         */
        void add_extent(uint64_t tid, uint64_t pg_xid, uint64_t lsn, const ExtentPtr data);

        /**
         * @brief Add a mapping from springtail XID to Postgres XID
         * @param pg_xid Postgres XID
         * @param xid springtail XID
         * @param md Metadata 
         */
        void commit(uint64_t pg_xid, uint64_t xid, WriteCacheTableSet::Metadata md);

        /**
         * @brief Add a mapping from springtail XID to Postgres XID
         * @param pg_xids Postgres XID
         * @param xid springtail XID
         * @param commit_ts postgres commit ts
         */
        void commit(std::vector<uint64_t> pg_xids, uint64_t xid, WriteCacheTableSet::Metadata md);

        /**
         * @brief Drop a table from the index
         * @param tid table ID
         * @param pg_xid Postgres XID
         */
        void drop_table(uint64_t tid, uint64_t pg_xid);

        /**
         * @brief Drop all data for a given XID
         * @param pg_xid Postgres XID
         */
        void abort(uint64_t pg_xid);

        /**
         * @brief Drop all data for a given XID
         * @param pg_xids Postgres XIDs
         */
        void abort(std::vector<uint64_t> pg_xids);

        //// RPC interface

        /**
         * @brief Get the table ids for a given XID
         * @param xid springtail XID
         * @param count number of items to return
         * @param cursor current offset into result set
         * @return std::vector<uint64_t>
         */
        std::vector<uint64_t> get_tids(uint64_t xid, uint32_t count, uint64_t &cursor);

        /**
         * @brief Evict extent from cache
         * @param tid table ID
         * @param xid springtail XID
         */
        void evict_table(uint64_t tid, uint64_t xid);

        /**
         * @brief Evict springtail XID from cache (and all data)
         * @param xid springtail XID
         */
        void evict_xid(uint64_t xid);

        /**
         * @brief Get the extents for a table at a given XID
         * @param tid table ID
         * @param xid springtail XID
         * @param count max. number of extents to fetch; done when count >= vector size
         * @param cursor In/Out cursor, in: set to 0 for start of range, out: current position
         * @param commit_ts out; postgres-reported commit ts of xid
         * @return std::vector<WriteCacheIndexExtentPtr>
         */
        std::vector<WriteCacheIndexExtentPtr> get_extents(uint64_t tid, uint64_t xid,
                                                          uint32_t count, uint64_t &cursor, WriteCacheTableSet::Metadata &md);

    private:
        /** Set of partitions to hold table data, enables more parallelism */
        std::vector<std::shared_ptr<WriteCacheTableSet>> _partitions;

        /** Number of partitions */
        int _num_partitions;

        /**
         * @brief Get the partition for a specific table ID
         * @param tid table ID
         * @return std::shared_ptr<WriteCacheTableSet>
         */
        std::shared_ptr<WriteCacheTableSet> _get_partition(uint64_t tid) {
            return _partitions[tid % _num_partitions];
        }
    };
    using WriteCacheIndexPtr = std::shared_ptr<WriteCacheIndex>;
}
