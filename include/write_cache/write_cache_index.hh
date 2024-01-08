#pragma once

#include <map>
#include <string>
#include <memory>
#include <vector>

//#include <write_cache/write_cache_table_set.hh>

namespace springtail {

    class WriteCacheTableSet;

    /**
     * @brief Row data structure; contains pkey, and data for a row
     */
    struct WriteCacheIndexRow {
        uint64_t xid;
        uint64_t xid_seq;
        std::string pkey;
        std::string data;
        bool delete_flag;

        WriteCacheIndexRow(const std::string &&data, const std::string &&pkey,
                           uint64_t xid, uint64_t xid_seq, bool delete_flag=false)
            : xid(xid), xid_seq(xid_seq), pkey(pkey), data(data), delete_flag(delete_flag)
        {}

        WriteCacheIndexRow(const std::string &&pkey, uint64_t xid, uint64_t xid_seq, bool delete_flag=true)
            : xid(xid), xid_seq(xid_seq), pkey(pkey), delete_flag(delete_flag)
        {}
    };

    /**
     * @brief Table change operation
     */
    struct WriteCacheIndexTableChange {
        uint64_t tid;
        uint64_t xid;
        uint64_t xid_seq;

        enum TableChangeOp {
            INVALID=0,
            TRUNCATE_TABLE = 1,
            SCHEMA_CHANGE = 2
        } op;

        struct Comparator {
            bool operator()(const std::shared_ptr<WriteCacheIndexTableChange> &lhs,
                            const std::shared_ptr<WriteCacheIndexTableChange> &rhs) const {
                if (lhs->tid < rhs->tid) { return true; }
                if (lhs->tid == rhs->tid && lhs->xid < rhs->xid) { return true; }
                if (lhs->tid == rhs->tid && lhs->xid == rhs->xid && lhs->xid_seq < rhs->xid_seq) { return true; }
                return false;
            }
        };

        WriteCacheIndexTableChange(uint64_t tid, uint64_t xid, uint64_t xid_seq, TableChangeOp op = TableChangeOp::INVALID)
            : tid(tid), xid(xid), xid_seq(xid_seq), op(op)
        {}
    };

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
        void add_row(uint64_t tid, uint64_t eid, std::shared_ptr<WriteCacheIndexRow> data);

        /**
         * @brief Get list of dirty table ids within xid range
         * @param start_xid start of xid range
         * @param end_xid end of xid range
         * @param count max number of items to return (may be less)
         * @return std::vector<int64_t> list of table IDs
         */
        std::vector<int64_t> get_tids(uint64_t start_xid, uint64_t end_xid, uint32_t count);

        /**
         * @brief Get list of dirty extents
         * @param tid table ID
         * @param start_xid start of xid range
         * @param end_xid end of xid range
         * @param count max number of items to return (may be less)
         * @param cursor cursor indicating current position
         * @return std::vector<int64_t> list of extent IDs
         */
        std::vector<int64_t> get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor);

        /**
         * @brief Get data rows
         * @param tid table ID
         * @param eid extent ID
         * @param start_xid start of xid range
         * @param end_xid end of xid range
         * @param count max number of items
         * @return std::vector<std::shared_ptr<WriteCacheIndexRow>>
         */
        std::vector<std::shared_ptr<WriteCacheIndexRow>> get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid, int count);

        /**
         * @brief Evict extent from cache
         * @param tid table ID
         * @param eid extent ID
         * @param start_xid start of xid range
         * @param end_xid end of xid range
         */
        void evict_extent(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Add a table change
         * @param change ptr to change struct
         */
        void add_table_change(std::shared_ptr<WriteCacheIndexTableChange> change);

        /**
         * @brief Get set of table changes for a table
         * @param tid table ID
         * @param start_xid start of xid range
         * @param end_xid end of xid range
         * @return std::vector<std::shared_ptr<WriteCacheIndexTableChange>>
         */
        std::vector<std::shared_ptr<WriteCacheIndexTableChange>> get_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Evict table changes between xid range
         * @param tid table ID
         * @param start_xid start of xid range
         * @param end_xid end of xid range
         */
        void evict_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

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
}