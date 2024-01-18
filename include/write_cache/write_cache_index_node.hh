#pragma once

#include <set>
#include <map>
#include <memory>
#include <variant>
#include <mutex>
#include <shared_mutex>
#include <cassert>

#include <common/tracking_allocator.hh>

namespace springtail {
    class WriteCacheIndexNode;
    typedef std::shared_ptr<WriteCacheIndexNode> WriteCacheIndexNodePtr;

    /**
     * @brief Generic node that is used to hold an ordered set of children of same type.
     * Used to build out XID Map Tree, top level is set of XIDs, then set of Tables then set of Extents, and then Rows.
     * Also used by the EID map, to map a set of Extents
     */
    class WriteCacheIndexNode {
    public:
        /** Compare IndexNodes for sorting based on ID */
        struct ComparatorID {
            bool operator()(const WriteCacheIndexNodePtr &lhs, const WriteCacheIndexNodePtr &rhs) const {
                return (lhs->id < rhs->id);
            }
        };

        enum IndexType : uint8_t {
            INVALID=0, // for searches
            ROOT=1,
            XID=2,
            TABLE=3,
            EXTENT=4
        };

        /** ID of entry, uint64_t for table and extent or XID, RowID for row */
        uint64_t id;

        /** type for sanity checking */
        IndexType type;

        /** shared mutex to protect children set */
        mutable std::shared_mutex mutex;

        /** Map holding key to next level of node */
        std::set<WriteCacheIndexNodePtr, ComparatorID, TrackingAllocator<WriteCacheIndexNodePtr>> children;

        /** Constructor for integer id */
        WriteCacheIndexNode(uint64_t id, IndexType type=IndexType::INVALID) : id(id), type(type) {};

        /** Find child node by int id, return nullptr if not exists */
        WriteCacheIndexNodePtr find(uint64_t id) const;

        /** Find child node by passed in node, return nullptr if not exists */
        WriteCacheIndexNodePtr find(WriteCacheIndexNodePtr entry) const;

        /** Find child node by id (uint64_t), if not exists then add */
        WriteCacheIndexNodePtr findAdd(uint64_t id, IndexType type);

        /** Find child node by node ptr, if not exists then add */
        WriteCacheIndexNodePtr findAdd(WriteCacheIndexNodePtr entry);

        /** Remove child node by ID */
        WriteCacheIndexNodePtr remove(uint64_t id);

        /** Remove child node by node ptr */
        WriteCacheIndexNodePtr remove(WriteCacheIndexNodePtr entry);

        /** Convert type to string for debugging */
        std::string type_to_str() const
        {
            switch(type) {
                case IndexType::ROOT: return "ROOT";
                case IndexType::XID: return "XID";
                case IndexType::TABLE: return "TABLE";
                case IndexType::EXTENT: return "EXTENT";
                default: return "INVALID";
            }
        }

        /** Dump entry as string for debugging */
        std::string dump() const
        {
            return fmt::format("{}:{}\n", type_to_str(), id);
        }

    private:
        /** Insert entry into children set, write lock must be held */
        WriteCacheIndexNodePtr _insert_child(WriteCacheIndexNodePtr entry);
    };

    /**
     * @brief Map of table ID to Map of row ID to WriteCacheRowMapPtr
     */
    class WriteCacheIndexRowMap {
    private:
        /**
         * @brief Comparator for the WriteCacheIndexRow
         */
        struct IndexRowComparator {
            // order by extent ID, xid, then row ID; allows for extent:xid range searches
            bool operator()(const  WriteCacheIndexRowPtr &lhs, const WriteCacheIndexRowPtr &rhs) const {
                return (lhs->eid < rhs->eid ||
                    (lhs->eid == rhs->eid && lhs->xid < rhs->xid) ||
                    (lhs->eid == rhs->eid && lhs->xid == rhs->xid &&
                     lhs->xid_seq < rhs->xid_seq) ||
                    (lhs->eid == rhs->eid && lhs->xid == rhs->xid &&
                     lhs->xid_seq == rhs->xid_seq && lhs->pkey < rhs->pkey));
            }
        };

        /** Table map entry, holds map from RowDataKey to row data (WriteCacheIndexRow) */
        struct Entry {
            /** Set of write cache index rows */
            std::set<WriteCacheIndexRowPtr, IndexRowComparator, TrackingAllocator<WriteCacheIndexRowPtr>> set;
            /** Set mutex */
            mutable std::shared_mutex entry_mutex;
        };
        typedef std::shared_ptr<Entry> EntryPtr;

        /** Map from TID to entry containing map<RowKey, RowData> */
        std::map<uint64_t, EntryPtr> map;

        /** Map's mutex */
        mutable std::shared_mutex map_mutex;

    public:

        /** Add an index_node at an xid for a specific ID */
        void add(uint64_t tid, uint64_t eid, uint64_t xid,
                 const std::vector<WriteCacheIndexRowPtr> &rows);

        /** Remove an entry for an tid for a given xid range; exclusive of start */
        void remove(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid);

        /** Retreive a set of rows for an xid range, return number of rows */
        int get(uint64_t tid, uint64_t eid,
                uint64_t start_xid, uint64_t end_xid, int count,
                std::vector<std::shared_ptr<WriteCacheIndexRow>> &result) const;

        void dump() const
        {
            // no locks held so may be inconsistent, use for debug only
            for (auto tid_itr = map.begin(); tid_itr != map.end(); tid_itr++) {
                std::cout << "Table ID: " << tid_itr->first << std::endl;
                EntryPtr entry = tid_itr->second;
                for (auto entry_itr = entry->set.begin(); entry_itr != entry->set.end(); entry_itr++) {
                    std::cout << (*entry_itr)->dump() << std::endl;
                }
            }
        }

    private:
        /** Helper, to add an index node at an xid for an ID; assumes a write lock exists on the external map */
        void _add(uint64_t tid, uint64_t eid, uint64_t xid,
                  const std::vector<WriteCacheIndexRowPtr> &rows,
                  std::unique_lock<std::shared_mutex> &map_lock);

        /** Helper to replace an existing row's data if it exists. Need to check xid_seq. */
        void _add_existing(EntryPtr entry, uint64_t eid, uint64_t xid,
                           const std::vector<WriteCacheIndexRowPtr> &rows);
    };
    typedef std::shared_ptr<WriteCacheIndexRowMap> WriteCacheIndexRowMapPtr;

}