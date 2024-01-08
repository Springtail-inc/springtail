#pragma once

#include <set>
#include <map>
#include <string>
#include <memory>
#include <variant>
#include <mutex>
#include <cassert>
#include <iostream>

#include <fmt/core.h>

#include "ThriftWriteCache.h"
#include "write_cache_types.h"

#include <write_cache/write_cache_index.hh>

namespace springtail {

    //struct WriteCacheIndexRow;
    //struct WriteCacheIndexTableChange;

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
        void add_row(uint64_t tid, uint64_t eid, std::shared_ptr<WriteCacheIndexRow> data);

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
                     std::vector<std::shared_ptr<WriteCacheIndexRow>> &result);

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
         * @brief Evict all rows for extent, fixup indexes
         * @param tid table ID
         * @param eid extent ID
         * @param start_xid starting XID (for range, exclusive)
         * @param end_xid   ending XID (for range, inclusive)
         */
        void evict_extent(uint64_t tid, uint64_t eid,
                          uint64_t start_xid, uint64_t end_xid);


        /**
         * @brief Add table change
         * @param change table change
         */
        void add_table_change(std::shared_ptr<WriteCacheIndexTableChange> change);

        /**
         * @brief Get set of table changes for table within xid range
         * @param tid table ID
         * @param start_xid starting XID (for range, exclusive)
         * @param end_xid ending XID (for range, inclusive)
         * @param changes
         */
        void get_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid, std::vector<std::shared_ptr<WriteCacheIndexTableChange>> &changes);

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

        /** convenience type for row ID, it is either the hashed primary key or full primary key */
        typedef std::string rid_t;

        /**
         * @brief Tree node. The node stores:
         *  - a parent ID p_id
         *  - a starting and ending xid range
         *  - an ID of the element represented by this node (row ID, extent ID or table ID)
         *  - a type (indicating the level: row, table, extent)
         *  - a comparator function that orders nodes first by start_xid, then by end_xid for
         *    XID range lookups
         *  - a set of children (next level of tree) of same type
         *
         *  Additionally, point lookups into this tree can be done using the table, and extent maps.
         *  Row data is stored in a separate map.  The lowest level of this tree holds a rid (row id)
         *  that can be used to index into the row_data map.
         */
        struct XidIdRange {

            /**
             * Comparator function
             * Also used to determine equality, so it includes comparison of id
             */
            struct Comparator {
                bool operator()(const std::shared_ptr<XidIdRange>& lhs, const std::shared_ptr<XidIdRange>& rhs) const {
                    if (!lhs || !rhs) {
                        return false;
                    }
                    if (lhs->start_xid < rhs->start_xid) { return true; }
                    if (lhs->start_xid == rhs->start_xid && lhs->end_xid < rhs->end_xid) { return true; }
                    if (lhs->start_xid == rhs->start_xid && lhs->end_xid == rhs->end_xid &&
                        lhs->id < rhs->id) { return true; }

                    return false;
                }
            };

            /** Type of index used within XidIdRange structure to indicate level of node in tree */
            enum IndexType : uint8_t {
                INVALID=0,
                TABLE=1,
                EXTENT=2,
                ROW=3
            };

            uint64_t start_xid; ///< starting XID for this range
            uint64_t end_xid;   ///< ending XID for this range
            uint64_t p_id;      ///< parent's ID, table or extent ID

            /** ID of entry, uint64_t for table and extent, rid_t for row */
            std::variant<uint64_t, rid_t> id;

            /** type of entry: table, extent or row */
            IndexType type;

            /** root of tree for children (next level down); empty for row nodes */
            std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, Comparator>> children;

            /**
             * Table/Extent XIDRange constructor; initialize children to an empty set
             */
            XidIdRange(uint64_t start_xid, uint64_t end_xid, IndexType type, uint64_t id, uint64_t p_id=-1)
                : start_xid(start_xid), end_xid(end_xid), p_id(p_id), id(id), type(type),
                  children(std::make_shared<std::set<std::shared_ptr<XidIdRange>, Comparator>>())
            {}

            /**
             * Row XidIdRange constructor; children set are always empty since this is bottom of the tree
             */
            XidIdRange(uint64_t xid, rid_t id, uint64_t p_id=-1)
                : start_xid(xid), end_xid(xid), p_id(p_id), id(id), type(IndexType::ROW), children({})
            {}

            /**
             * XidIdRange constructor; for use in comparison, just initialize xid range,
             * uninitialized id should compare < anything that is actually set (for variants)
             */
            XidIdRange(uint64_t start_xid, uint64_t end_xid)
                : start_xid(start_xid), end_xid(end_xid), p_id(-1), type(IndexType::INVALID)
            {}

            /**
             * @brief Get the row id (rid_t) from the id variant
             * @return rid_t id
             */
            inline rid_t get_rid() {
                assert(type == IndexType::ROW);
                return std::get<rid_t>(id);
            }

            /**
             * @brief Get the id (uint64_t) from the id variant
             * @return uint64_t id
             */
            inline uint64_t get_id() {
                assert(type != IndexType::ROW);
                return std::get<uint64_t>(id);
            }

            inline void dump() {
                std::string _type;
                std::string _id;
                if (type == IndexType::TABLE) {
                    _type = "table";
                    _id = fmt::format("{}", get_id());
                } else if (type == IndexType::EXTENT) {
                    _type = "extent";
                    _id = fmt::format("{}", get_id());
                } else if (type == IndexType::ROW) {
                    _type = "row";
                    _id = get_rid();
                }
                std::cout << "Level: " << _type << " ID: " << _id << " P_ID: " << p_id
                          << " XIDs: " << start_xid << ":" << end_xid << std::endl;

            }
        };

        /** convenience type for XidIdRange set */
        typedef std::set<std::shared_ptr<XidIdRange>, XidIdRange::Comparator> XidIdRangeSet_t;

        /**
         * @brief Key for row data.  Ordered by tid, then eid, then xid, then rid
         * Ordering xid before rid allows for easier eviction based on eid.
         */
        struct RowDataKey {
            uint64_t tid;
            uint64_t eid;
            uint64_t xid;
            rid_t    rid;

            struct Comparator {
                bool operator()(const RowDataKey &lhs, const RowDataKey &rhs) const {
                    if (lhs.tid < rhs.tid) { return true; }
                    if (lhs.tid == rhs.tid && lhs.eid < rhs.eid) { return true; }
                    if (lhs.tid == rhs.tid && lhs.eid == rhs.eid &&
                        lhs.xid < rhs.xid) { return true; }
                    if (lhs.tid == rhs.tid && lhs.eid == rhs.eid &&
                        lhs.xid == rhs.xid && lhs.rid < rhs.rid) { return true; }
                    return false;
                }
            };

            RowDataKey(uint64_t tid, uint64_t eid, uint64_t xid, rid_t rid={}) :
                tid(tid), eid(eid), xid(xid), rid(rid) {}
        };

        /** root of tree, each level points to another set of ids sorted by max xid */
        std::shared_ptr<XidIdRangeSet_t> _table_root;

        /** map of TID to set of EID XID ranges; indexed by tid */
        std::map<uint64_t, std::shared_ptr<XidIdRange>> _tid_map;

        /** map of EID to the set of XID row ranges; indexed by tid:eid */
        std::map<std::string, std::shared_ptr<XidIdRange>> _eid_map;

        /** Holds the row data, indexed by: RowDataKey (tid:eid:xid:rid) */
        std::map<RowDataKey, std::shared_ptr<WriteCacheIndexRow>, RowDataKey::Comparator> _row_data_map;

        /** Holds the table change data by xid */
        std::multiset<std::shared_ptr<WriteCacheIndexTableChange>, WriteCacheIndexTableChange::Comparator> _table_change_set;

        /** Lock for _row_data_map */
        std::mutex _row_data_mutex;

        // XXX TBD
        std::mutex _eid_mutex;

        /**
         * @brief Get row id from primary key; hashes primary key or uses primary key as ID
         * @param pkey Primary key
         * @return rid_t row id
         */
        rid_t _get_rid_from_row(const std::string &pkey);

        /**
         * @brief Insert row data
         * @param tid table ID
         * @param eid extent ID
         * @param rid row ID
         * @param data row data
         * @return true  row entry exists (xid ranges need not be updated)
         * @return false row entry does not exist (xid ranges need to be updated)
         */
        bool _insert_row_data(uint64_t tid, uint64_t eid, rid_t rid, std::shared_ptr<WriteCacheIndexRow> data);

        /**
         * @brief Insert row entry into extent index; may also insert into table map
         * @param tid table ID
         * @param eid extent ID
         * @param rid row ID
         * @param xid row XID
         */
        void _insert_extent_index(uint64_t tid, uint64_t eid, rid_t rid, uint64_t xid);

        /**
         * @brief Insert row into table index
         * @param tid table ID
         * @param xid row XID
         * @param eid_xid_range extent XID range (node ptr)
         */
        void _insert_table_index(uint64_t tid, uint64_t xid, std::shared_ptr<XidIdRange> eid_xid_range);

        /**
         * @brief Fixup XID start/end for xid_range node within provided set
         *        Checks for start or end XID changes; to properly fixup the node, it must
         *        be removed from the parent's children set and re-added
         * @param id  either tid or eid (not rid)
         * @param start_xid new start XID
         * @param end_xid new end XID
         * @param child_node ptr to xid range node (from index map)
         * @param parent_set set to search for xid range node (parent set)
         * @param insert boolean, if true then fixup for insert, if false then for removal
         */
        void _fixup_set(uint64_t id, uint64_t start_xid, uint64_t end_xid,
                        std::shared_ptr<XidIdRange> child_node,
                        std::shared_ptr<XidIdRangeSet_t> parent_set, bool insert);

        /**
         * @brief Fixup XID start/end for xid_range node within an extent; fixes up
         *        table index too if necessary
         * @param tid table ID
         * @param eid extent ID
         * @param xid row XID
         * @param extent_node ptr to extent xid range node
         * @param insert boolean, if true then fixup range for inserting new entry, if false then removing entry
         */
        void _fixup_eid_range(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid,
                              std::shared_ptr<XidIdRange> extent_node, bool insert);

        /**
         * @brief Remove row data from cache
         * @param tid table ID
         * @param eid extent ID
         * @param start_xid start XID
         * @param end_xid end XID
         */
        void _remove_row_data(uint64_t tid, uint64_t eid,
                              uint64_t start_xid, uint64_t end_xid);


        /**
         * @brief Check if node's start xid range needs to be adjusted
         *        Comparing node's start xid with passed in start/end range
         * @param node node to compare
         * @param start_xid start of new range
         * @param end_xid   end of new range
         * @param expand_range whether the range is expanding (insert) or shrinking (removal)
         * @return true if start/end range requires node to be fixed up
         * @return false if start/end range does not require node to be fixed up
         */
        bool _check_start_range(std::shared_ptr<XidIdRange> node,
                                uint64_t start_xid, uint64_t end_xid, bool expand_range);

        /**
         * @brief Check if node's end xid range needs to be adjusted
         *        Comparing node's end xid with passed in start/end range
         * @param node node to compare
         * @param start_xid start of new range
         * @param end_xid   end of new range
         * @param expand_range whether the range is expanding (insert) or shrinking (removal)
         * @return true if start/end range requires node to be fixed up
         * @return false if start/end range does not require node to be fixed up
         */
        bool _check_end_range(std::shared_ptr<XidIdRange> node,
                              uint64_t start_xid, uint64_t end_xid, bool expand_range);

        /**
         * @brief Utility helper to dump tree from provided root
         * @param set Root of tree to dump (call from public dump())
         */
        void _dump(std::shared_ptr<XidIdRangeSet_t> set);

        /**
         * @brief Get extent key for extent map
         * @param tid table id
         * @param eid extent id
         * @return std::string extent id key
         */
        inline std::string _get_extent_key(uint64_t tid, uint64_t eid) {
            return fmt::format("{}:{}", tid, eid);
        }
    };
}