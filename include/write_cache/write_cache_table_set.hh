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
            uint64_t start_xid;
            uint64_t end_xid;
            uint64_t p_id;

            std::variant<uint64_t, rid_t> id;

            IndexType type;

            struct XidIdRangeComparator {
                bool operator()(const std::shared_ptr<XidIdRange>& lhs, const std::shared_ptr<XidIdRange>& rhs) const {
                    if (!lhs || !rhs) {
                        return false;
                    }
                    if (lhs->start_xid < rhs->start_xid) { return true; }
                    if (lhs->start_xid == rhs->start_xid && lhs->end_xid < rhs->end_xid) { return true; }

                    return false;
                }
            };

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

        WriteCacheTableSet();

        void add_row(uint64_t tid, uint64_t eid, std::shared_ptr<WriteCacheIndexRow> data);

        void get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid);//, int count);

        void dump();

    private:
        /** root of tree, each level points to another set of ids sorted by max xid */
        std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>> _table_root;

        std::map<uint64_t, std::shared_ptr<XidIdRange>> _tid_map;

        /** Holds map of EID to the set of XID row ranges; indexed by tid:eid */
        std::map<std::string, std::shared_ptr<XidIdRange>> _eid_map;

        /** Holds the row data, indexed by string: 'tid:rid:xid' */
        std::map<std::string, std::shared_ptr<WriteCacheIndexRow>> _row_data_map;

        std::mutex _row_data_mutex;
        std::mutex _eid_mutex;

        /**
         * @brief Get row id from primary key
         * @param pkey Primary key
         * @return rid_t row id
         */
        rid_t _get_rid_from_row(const std::string &pkey);

        bool _insert_row_data(uint64_t tid, rid_t rid, std::shared_ptr<WriteCacheIndexRow> data);

        void _insert_eid_map(uint64_t tid, uint64_t eid, rid_t rid, uint64_t xid);

        void _insert_tid_map(uint64_t tid, uint64_t xid, std::shared_ptr<XidIdRange> eid_xid_range);

        void _fixup_set(uint64_t id, uint64_t xid,
                        std::shared_ptr<XidIdRange> xid_range,
                        std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>> set);

        void _dump(std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>> set);
    };
}