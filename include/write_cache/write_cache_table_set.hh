#pragma once

#include <set>
#include <map>
#include <string>
#include <memory>
#include <variant>
#include <mutex>

#include "ThriftWriteCache.h"
#include "write_cache_types.h"

namespace springtail {

    struct WriteCacheIndexRow;

    /** Encapsulation of an index for a set of table partitions */
    class WriteCacheTableSet {
    public:
        /** convenience type for row ID, it is either the hashed primary key or full primary key */
        typedef std::string rid_t;

        /** Type of index used within XidIdRange structure to indicate level of node in tree */
        enum IndexType : uint8_t {
            TABLE=0,
            EXTENT=1,
            ROW=2
        };

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
            uint64_t start_xid; ///< starting XID for this range
            uint64_t end_xid;   ///< ending XID for this range
            uint64_t p_id;      ///< parent's ID, table or extent ID

            /** ID of entry, uint64_t for table and extent, rid_t for row */
            std::variant<uint64_t, rid_t> id;

            /** Level type: row, extent, table */
            IndexType type;

            /**
             * Comparator function
             * Also used to determine equality, so it includes comparison of id
             */
            struct XidIdRangeComparator {
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

            /** root of tree for children (next level down); empty for row nodes */
            std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRangeComparator>> children;

            /**
             * Table/Extent XIDRange constructor; initialize children to an empty set
             */
            XidIdRange(uint64_t start_xid, uint64_t end_xid, IndexType type, uint64_t id, uint64_t p_id=-1)
                : start_xid(start_xid), end_xid(end_xid), p_id(p_id), id(id), type(type),
                  children(std::make_shared<std::set<std::shared_ptr<XidIdRange>, XidIdRangeComparator>>())
            {}

            /**
             * Row XidIdRange constructor; children set are always empty since this is bottom of the tree
             */
            XidIdRange(uint64_t start_xid, uint64_t end_xid, rid_t id, uint64_t p_id=-1)
                : start_xid(start_xid), end_xid(end_xid), p_id(p_id), id(id), type(IndexType::ROW), children({})
            {}

            /**
             * XidIdRange constructor for use in comparison, just initialize xid range
             */
            XidIdRange(uint64_t start_xid, uint64_t end_xid) : start_xid(start_xid), end_xid(end_xid)
            {}
        };

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
         */
        void get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid);//, int count);

        /**
         * @brief Helper utility to dump from _table_root
         */
        void dump();

    private:
        /** root of tree, each level points to another set of ids sorted by max xid */
        std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>> _table_root;

        /** map of TID to set of EID XID ranges; indexed by tid */
        std::map<uint64_t, std::shared_ptr<XidIdRange>> _tid_map;

        /** map of EID to the set of XID row ranges; indexed by tid:eid */
        std::map<std::string, std::shared_ptr<XidIdRange>> _eid_map;

        /** Holds the row data, indexed by string: 'tid:rid:xid' */
        std::map<std::string, std::shared_ptr<WriteCacheIndexRow>> _row_data_map;

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
         *        Checks for start or end XID changes
         * @param id  either tid or eid (not rid)
         * @param xid row XID
         * @param xid_range xid range node (from index map)
         * @param set set to search for xid range node
         */
        void _fixup_set(uint64_t id, uint64_t xid,
                        std::shared_ptr<XidIdRange> xid_range,
                        std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>> set);

        /**
         * @brief Fixup XID start/end for xid_range node within an extent; fixes up
         *        table index too if necessary
         * @param tid table ID
         * @param eid extent ID
         * @param xid row XID
         * @param xid_range ptr to extent xid range node
         */
        void _fixup_eid_range(uint64_t tid, uint64_t eid, uint64_t xid, std::shared_ptr<XidIdRange> xid_range);

        /**
         * @brief Utility helper to dump tree from provided root
         * @param set Root of tree to dump (call from public dump())
         */
        void _dump(std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>> set);
    };
}