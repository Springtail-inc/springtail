#pragma once

#include <map>
#include <string>
#include <memory>
#include <vector>

#include <write_cache/write_cache_table_set.hh>

namespace springtail {

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

/*
        std::vector<XidIdRange> get_tids(uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor);

        std::vector<XidIdRange> get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor);

        std::vector<XidIdRange> get_rids(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor);

        std::vector<std::shared_ptr<RowData>> get_rows(std::vector<rid_t> row_ids);
*/

    private:
        /** Set of partitions to hold table data, enables more parallelism */
        std::vector<std::shared_ptr<WriteCacheTableSet>> _partitions;
        int _num_partitions;

        std::shared_ptr<WriteCacheTableSet> _get_partition(uint64_t tid) {
            return _partitions[tid % _num_partitions];
        }
    };
}