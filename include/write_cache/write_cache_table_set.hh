#pragma once

#include <set>
#include <map>
#include <string>
#include <memory>
#include <variant>
#include <mutex>
#include <shared_mutex>
#include <cassert>
#include <iostream>

#include <fmt/core.h>


#include "ThriftWriteCache.h"
#include "write_cache_types.h"

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_index_node.hh>

namespace springtail {

    /** Encapsulation of an index for a set of table partitions */
    class WriteCacheTableSet {
    public:
        /**
         * @brief Construct a new Write Cache Table Set object
         */
        WriteCacheTableSet();

        /**
         * @brief Add new row to index, updates all maps and sets
         * @param tid table ID
         * @param eid extent ID
         * @param data row data
         */
        void add_rows(uint64_t tid, uint64_t eid, const std::vector<WriteCacheIndexRowPtr> &data);

        /**
         * @brief Get the row data
         * @param tid table ID
         * @param eid extent ID
         * @param start_xid starting XID (for range, exclusive)
         * @param end_xid   ending XID (for range, inclusive)
         * @param count number of items to return; may be less
         * @param result reference to result vector, may be partially filled
         * @return int number of elements added
         */
        int get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid, int count,
                     std::vector<WriteCacheIndexRowPtr> &result);

        /**
         * @brief Get a list of table IDs
         * @param start_xid starting XID (for range, exclusive)
         * @param end_xid   ending XID (for range, inclusive)
         * @param count number of items to return; may be less
         * @param result reference to result vector (thrift only supports int64, so that is what we use)
         * @return int number of elements added
         */
        int get_tids(uint64_t start_xid, uint64_t end_xid,
                     int count, std::vector<int64_t> &result);

        /**
         * @brief Get a list of table IDs
         * @param tid table ID
         * @param start_xid starting XID (for range, exclusive)
         * @param end_xid   ending XID (for range, inclusive)
         * @param count number of items to return; may be less
         * @param result reference to result vector (thrift only supports int64, so that is what we use)
         * @return int number of elements added
         */
        int get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                     int count, std::vector<int64_t> &result);

        /**
         * @brief Evict all rows for table, fixup indexes
         *        Assumption: no insert for row data for any XID within range will occur while eviction is in progress
         * @param tid table ID
         * @param start_xid starting XID (for range, exclusive)
         * @param end_xid   ending XID (for range, inclusive)
         */
        void evict_table(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Add table change
         * @param change table change
         */
        void add_table_change(WriteCacheIndexTableChangePtr change);

        /**
         * @brief Get set of table changes for table within xid range
         * @param tid table ID
         * @param start_xid starting XID (for range, exclusive)
         * @param end_xid ending XID (for range, inclusive)
         * @param changes result, list of changes
         */
        void get_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                               std::vector<WriteCacheIndexTableChangePtr> &changes);

        /**
         * @brief Remove table changes from cache for xid range
         * @param tid table id
         * @param start_xid starting XID
         * @param end_xid ending XID (inclusive)
         */
        void evict_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /**
         * @brief Helper utility to dump from _table_root
         */
        void dump();

    private:
        /**
         * @brief Key for row data.  Ordered by tid, then eid, then xid, then rid
         * Ordering xid before rid allows for easier eviction based on eid.
         */


        /** root of tree, each level points to another set of ids sorted by max xid */
        WriteCacheIndexNodePtr _xid_root;

        /** map of tid to row data */
        WriteCacheIndexRowMapPtr _row_map;

        /** holds the table change data by xid */
        std::multiset<WriteCacheIndexTableChangePtr, WriteCacheIndexTableChange::Comparator> _table_change_set;

        /** lock for _table_change_set */
        std::shared_mutex _table_change_mutex;

        /**
         * @brief Get extent key for extent map
         * @param tid table id
         * @param eid extent id
         * @return std::string extent id key
         */
        inline std::string _get_extent_key(uint64_t tid, uint64_t eid) {
            return fmt::format("{}:{}", tid, eid);
        }

        /**
         * @brief Get row id from primary key; hashes primary key or uses primary key as ID
         * @param pkey Primary key
         * @return std::string row id
         */
        std::string _get_row_key(const std::string &pkey);

        /**
         * @brief Insert row IDs into table; populates XID, TID, EID, and eid_map
         * @param tid  Table ID
         * @param eid  Extent ID
         * @param xid  XID
         * @param rids list of row IDs
         */
        void _insert_rows(uint64_t tid, uint64_t eid, uint64_t xid, const std::vector<std::string> &rids);

        /**
         * @brief Fetch at most count unique IDs from the node passed in
         * @param node   node to search
         * @param count  max number of entries to return
         * @param result set of unique entries (in/out)
         * @return int   number of entries found
         */
        int _fetch_ids(WriteCacheIndexNodePtr node, int count, std::set<uint64_t> &result);

        /**
         * @brief Utility helper to dump tree from provided root
         * @param node Root of tree to dump (called from public dump())
         */
        void _dump(WriteCacheIndexNodePtr node);
    };
    typedef std::shared_ptr<WriteCacheTableSet> WriteCacheTableSetPtr;
}