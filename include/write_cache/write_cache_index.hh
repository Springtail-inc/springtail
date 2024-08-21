#pragma once

#include <map>
#include <string>
#include <memory>
#include <vector>

#include <fmt/core.h>

#include <write_cache/write_cache_index_common.hh>
#include <write_cache/write_cache_table_set.hh>

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
         * @brief Add row to the index
         * @param tid table id
         * @param eid extent id
         * @param data row data
         */
        void add_rows(uint64_t tid, uint64_t eid, const std::vector<WriteCacheIndexRowPtr> &data);

        /**
         * @brief Get list of dirty table ids within xid range
         * @param start_xid start of xid range (exclusive)
         * @param end_xid end of xid range (inclusive)
         * @param count max number of items to return (may be less)
         * @param cursor cursor indicating current position
         * @return std::vector<int64_t> list of table IDs
         */
        std::vector<int64_t> get_tids(uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor);

        /**
         * @brief Get list of dirty extents
         * @param tid table ID
         * @param start_xid start of xid range (exclusive)
         * @param end_xid end of xid range (inclusive)
         * @param count max number of items to return (may be less)
         * @param cursor cursor indicating current position
         * @return std::vector<int64_t> list of extent IDs
         */
        std::vector<int64_t> get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor);

        /**
         * @brief Get data rows
         * @param tid table ID
         * @param eid extent ID
         * @param start_xid start of xid range (exclusive)
         * @param end_xid end of xid range (inclusive)
         * @param count max number of items
         * @param cursor cursor indicating current position
         * @return std::vector<std::shared_ptr<WriteCacheIndexRow>>
         */
        std::vector<WriteCacheIndexRowPtr> get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid,
                                                    uint64_t end_xid, uint32_t count, uint64_t &cursor);

        /**
         * @brief Evict extent from cache
         * @param tid table ID
         * @param start_xid start of xid range (exclusive)
         * @param end_xid end of xid range (inclusive)
         */
        void evict_table(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Add a table change
         * @param change ptr to change struct
         */
        void add_table_change(WriteCacheIndexTableChangePtr change);

        /**
         * @brief Get set of table changes for a table
         * @param tid table ID
         * @param start_xid start of xid range (exclusive)
         * @param end_xid end of xid range (inclusive)
         * @return std::vector<std::shared_ptr<WriteCacheIndexTableChange>>
         */
        std::vector<std::shared_ptr<WriteCacheIndexTableChange>> get_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Evict table changes between xid range
         * @param tid table ID
         * @param start_xid start of xid range (exclusive)
         * @param end_xid end of xid range (inclusive)
         */
        void evict_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Set clean flag on extent under table
         * @param tid table ID
         * @param eid extent ID
         * @param start_xid start of xid range (exclusive)
         * @param end_xid end of xid range (inclusive)
         */
        void set_clean_flag(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Reset (unset) clean flag on all extents under table
         * @param tid table ID
         * @param start_xid start of xid range (exclusive)
         * @param end_xid end of xid range (inclusive)
         */
        void reset_clean_flag(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

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