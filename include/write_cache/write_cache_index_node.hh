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
        std::shared_mutex mutex;

        /** Map holding key to next level of node */
        std::set<WriteCacheIndexNodePtr, ComparatorID, TrackingAllocator<WriteCacheIndexNodePtr>> children;

        /** Constructor for integer id */
        WriteCacheIndexNode(uint64_t id, IndexType type=IndexType::INVALID) : id(id), type(type) {};

        /** Find child node by int id, return nullptr if not exists */
        inline WriteCacheIndexNodePtr find(uint64_t id)
        {
            auto entry = std::make_shared<WriteCacheIndexNode>(id);
            return find(entry);
        }

        /** Find child node by passed in node, return nullptr if not exists */
        inline WriteCacheIndexNodePtr find(WriteCacheIndexNodePtr entry)
        {
            std::shared_lock<std::shared_mutex> lock{mutex};
            auto itr = children.find(entry);
            if (itr == children.end()) {
                return nullptr;
            }
            return (*itr);
        }

        /** Find child node by id (uint64_t), if not exists then add */
        inline WriteCacheIndexNodePtr findAdd(uint64_t id, IndexType type)
        {
            auto entry = std::make_shared<WriteCacheIndexNode>(id, type);
            return findAdd(entry);
        }

        /** Find child node by node ptr, if not exists then add */
        WriteCacheIndexNodePtr findAdd(WriteCacheIndexNodePtr entry)
        {
            // first try to obtain a write lock
            std::unique_lock<std::shared_mutex> write_lock{mutex, std::try_to_lock};
            if (write_lock.owns_lock()) {
                // got the write lock
                return _insert_child(entry);
            }

            // try lock failed, fall back to read_lock
            std::shared_lock<std::shared_mutex> read_lock{mutex};
            auto itr = children.find(entry);
            if (itr != children.end()) {
                // found entry return it
                return (*itr);
            }

            // not found, insert entry, fall back to write lock
            read_lock.unlock();

            std::unique_lock<std::shared_mutex> new_write_lock{mutex};
            return _insert_child(entry);
        }

        /** Remove child node by ID */
        inline WriteCacheIndexNodePtr remove(uint64_t id)
        {
            auto entry = std::make_shared<WriteCacheIndexNode>(id);
            return remove(entry);
        }

        /** Remove child node by node ptr */
        inline WriteCacheIndexNodePtr remove(WriteCacheIndexNodePtr entry)
        {
            std::unique_lock<std::shared_mutex> write_lock{mutex};
            auto itr = children.find(entry);
            if (itr == children.end()) {
                return nullptr;
            }
            children.erase(itr);
            return (*itr);
        }

        /** Convert type to string for debugging */
        std::string type_to_str()
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
        std::string dump()
        {
            return fmt::format("{}:{}\n", type_to_str(), id);
        }

    private:
        /** Insert entry into children set, write lock must be held */
        WriteCacheIndexNodePtr _insert_child(WriteCacheIndexNodePtr entry)
        {

            auto itr = children.find(entry);
            if (itr != children.end()) {
                // entry now exists, return it
                return (*itr);
            }
            // insert entry and return it
            children.insert(entry);
            return entry;
        }
    };

    /**
     * @brief Map of table ID to Map of row ID to WriteCacheRowMapPtr
     */
    class WriteCacheIndexRowMap {
    protected:
        /**
         * @brief Row key in row map.  omits TID as TID is in top-level map
         */
        struct RowDataKey {
            uint64_t eid;
            uint64_t xid;
            std::string rid;

            struct Comparator {
                // order by extent ID, xid, then row ID; allows for extent:xid range searches
                bool operator()(const RowDataKey &lhs, const RowDataKey &rhs) const {
                    if (lhs.eid < rhs.eid) { return true; }
                    if (lhs.eid == rhs.eid &&
                        lhs.xid < rhs.xid) { return true; }
                    if (lhs.eid == rhs.eid &&
                        lhs.xid == rhs.xid && lhs.rid < rhs.rid) { return true; }
                    return false;
                }
            };

            RowDataKey(uint64_t eid, uint64_t xid, const std::string &rid={}) :
                eid(eid), xid(xid), rid(rid) {}

            std::string dump() const
            {
                return fmt::format("EID:{} XID:{} RID:{}\n", eid, xid, rid);
            }
        };

        /** Table map entry, holds map from RowDataKey to row data (WriteCacheIndexRow) */
        struct Entry {
            /** Map from RowDataKey to WriteCacheIndexRow */
            std::map<RowDataKey, WriteCacheIndexRowPtr, RowDataKey::Comparator, TrackingAllocator<std::pair<const RowDataKey,WriteCacheIndexRowPtr>>> map;
            /** Map's mutex */
            std::shared_mutex entry_mutex;
        };
        typedef std::shared_ptr<Entry> EntryPtr;

        /** Map from TID to entry containing map<RowKey, RowData> */
        std::map<uint64_t, EntryPtr> map;

        /** Map's mutex */
        std::shared_mutex map_mutex;

    public:

        /** Add an index_node at an xid for a specific ID */
        void add(uint64_t tid, uint64_t eid, uint64_t xid,
                 const std::vector<std::string> &rids,
                 const std::vector<WriteCacheIndexRowPtr> &rows)
        {
            std::unique_lock<std::shared_mutex> write_lock{map_mutex, std::try_to_lock};
            if (write_lock.owns_lock()) {
                // got write lock
                _add(tid, eid, xid, rids, rows, write_lock);
                return;
            }

            // didn't get the write lock, get read lock, check
            std::shared_lock<std::shared_mutex> read_lock{map_mutex};
            auto itr = map.find(tid);
            if (itr != map.end()) {
                // found existing entry in top level map; add to existing entry
                EntryPtr entry = itr->second;
                read_lock.unlock();
                _add_existing(entry, eid, xid, rids, rows);
                return;
            }

            // not found in top level map, get write lock at top level and try again...
            std::unique_lock<std::shared_mutex> map_write_lock{map_mutex};
            _add(tid, eid, xid, rids, rows, map_write_lock);
            return;
        }

        /** Remove an entry for an tid for a given xid range; exclusive of start */
        void remove(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid)
        {
            // lock top level map and search for id
            std::shared_lock<std::shared_mutex> read_lock{map_mutex};
            auto itr = map.find(tid);
            if (itr == map.end()) {
                // not found
                return;
            }

            // found, unlock top level map
            EntryPtr entry = itr->second;
            read_lock.unlock();

            // lock entry map and search for row
            RowDataKey rkey(eid, start_xid+1);

            std::unique_lock<std::shared_mutex> entry_lock{entry->entry_mutex};
            auto start_itr = entry->map.lower_bound(rkey);
            if (start_itr == entry->map.end()) {
                // not found
                return;
            }

            // start at start_itr and move forward to find end of xid range
            auto end_itr = start_itr;
            while (end_itr != entry->map.end() && end_itr->second->xid <= end_xid) {
                end_itr++;
            }

            // found, erase entry and check if map is empty
            entry->map.erase(start_itr, end_itr);
            bool is_empty = entry->map.empty();
            entry_lock.unlock();

            if (!is_empty) {
                return;
            }

            // if map is empty, write lock top level map
            std::unique_lock<std::shared_mutex> write_lock{map_mutex};
            itr = map.find(tid);
            if (itr == map.end()) {
                // not found
                return;
            }

            // write lock entry and check again that it is empty; if so remove entry
            entry = itr->second;
            std::unique_lock<std::shared_mutex> entry_write_lock{entry->entry_mutex};
            if (entry->map.empty()) {
                map.erase(itr);
            }

            return;
        }

        /** Retreive a set of rows for an xid range, return number of rows */
        int get(uint64_t tid, uint64_t eid,
                uint64_t start_xid, uint64_t end_xid, int count,
                std::vector<std::shared_ptr<WriteCacheIndexRow>> &result)
        {
            // lock table map
            std::shared_lock<std::shared_mutex> tid_lock{map_mutex};
            auto itr = map.find(tid);
            if (itr == map.end()) {
                return 0;
            }
            EntryPtr entry = itr->second;
            tid_lock.unlock();

            // lookup row; exclusive of start
            RowDataKey rkey(eid, start_xid+1);
            std::shared_lock<std::shared_mutex> entry_lock{entry->entry_mutex};
            auto entry_itr = entry->map.lower_bound(rkey);

            // iterate through rows to find those within the xid range
            int added=0;
            while (entry_itr != entry->map.end() &&
                   added < count && entry_itr->second->xid <= end_xid) {
                result.push_back(entry_itr->second);
                added++;
                entry_itr++;
            }

            return added;
        }

        void dump()
        {
            // no locks held so may be inconsistent, use for debug only
            for (auto tid_itr = map.begin(); tid_itr != map.end(); tid_itr++) {
                std::cout << "Table ID: " << tid_itr->first << std::endl;
                EntryPtr entry = tid_itr->second;
                for (auto entry_itr = entry->map.begin(); entry_itr != entry->map.end(); entry_itr++) {
                    std::cout << entry_itr->first.dump();
                }
            }
        }

    private:
        /** Helper, to add an index node at an xid for an ID; assumes a write lock exists on the external map */
        void _add(uint64_t tid, uint64_t eid, uint64_t xid,
                  const std::vector<std::string> &rids,
                  const std::vector<WriteCacheIndexRowPtr> &rows,
                  std::unique_lock<std::shared_mutex> &map_lock)
        {
            // write lock is held on map mutex
            auto itr = map.find(tid);
            if (itr != map.end()) {
                // entry found in outer map
                EntryPtr entry = itr->second;
                map_lock.unlock();
                _add_existing(entry, eid, xid, rids, rows);
                return;
            } else {
                // no entry found in outer map, create an entry and update that entry's map
                EntryPtr entry = std::make_shared<Entry>();
                for (int i = 0; i < rids.size(); i++) {
                    RowDataKey rkey(eid, xid, rids[i]);
                    entry->map.insert({rkey, rows[i]});
                }
                map.insert({tid, entry});
                return;
            }
        }

        /** Helper to replace an existing row's data if it exists. Need to check xid_seq. */
        void _add_existing(EntryPtr entry, uint64_t eid, uint64_t xid,
                           const std::vector<std::string> &rids,
                           const std::vector<WriteCacheIndexRowPtr> &rows)
        {
            // add entry if key doesn't exist
            std::unique_lock<std::shared_mutex> entry_lock{entry->entry_mutex};
            for (int i = 0; i < rids.size(); i++) {
                RowDataKey rkey(eid, xid, rids[i]);

                // try and insert into the map, if entry exists, won't insert
                auto p = entry->map.insert_or_assign(rkey, rows[i]);
                if (!p.second) {
                    // an entry already exists, so new data was not inserted
                    // p.first holds node (pair<key,value>)
                    // check xid seq of existing element to see if an update is needed
                    // if so, then check the xid ranges to see if they need updating
                    assert(p.first->second->xid == xid);

                    if (p.first->second->xid_seq < rows[i]->xid_seq) {
                        entry->map.emplace_hint(p.first, rkey, rows[i]);
                    }
                }
            }
        }
    };

    typedef std::shared_ptr<WriteCacheIndexRowMap> WriteCacheIndexRowMapPtr;

}