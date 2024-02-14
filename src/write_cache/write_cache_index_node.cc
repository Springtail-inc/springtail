#include <memory>
#include <mutex>

#include <common/logging.hh>

#include <write_cache/write_cache_table_set.hh>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_index_node.hh>

namespace springtail {
        /** Find child node by int id, return nullptr if not exists */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::find(uint64_t id) const
    {
        auto entry = std::make_shared<WriteCacheIndexNode>(id);
        return find(entry);
    }

    /** Find child node by passed in node, return nullptr if not exists */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::find(WriteCacheIndexNodePtr entry) const
    {
        std::shared_lock<std::shared_mutex> lock{mutex};
        auto itr = children.find(entry);
        if (itr == children.end()) {
            return nullptr;
        }
        return (*itr);
    }

    /** Find child node by id (uint64_t), if not exists then add */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::findAdd(uint64_t id, IndexType type)
    {
        auto entry = std::make_shared<WriteCacheIndexNode>(id, type);
        return findAdd(entry);
    }

    /** Find child node by node ptr, if not exists then add */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::findAdd(WriteCacheIndexNodePtr entry)
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
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::remove(uint64_t id)
    {
        auto entry = std::make_shared<WriteCacheIndexNode>(id);
        return remove(entry);
    }

    /** Remove child node by node ptr */
    WriteCacheIndexNodePtr
    WriteCacheIndexNode::remove(WriteCacheIndexNodePtr entry)
    {
        std::unique_lock<std::shared_mutex> write_lock{mutex};
        auto itr = children.find(entry);
        if (itr == children.end()) {
            return nullptr;
        }
        WriteCacheIndexNodePtr p = (*itr);
        children.erase(itr);
        return p;
    }

    WriteCacheIndexNodePtr
    WriteCacheIndexNode::_insert_child(WriteCacheIndexNodePtr entry)
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

    /** Add an index_node at an xid for a specific ID */
    void
    WriteCacheIndexRowMap::add(uint64_t tid, uint64_t eid, uint64_t xid,
                               const std::vector<WriteCacheIndexRowPtr> &rows)
    {
        std::shared_ptr<std::shared_mutex> map_mutex = _get_mutex(tid);
        std::shared_ptr<std::map<uint64_t, EntryPtr>> map = _get_map(tid);

        std::unique_lock<std::shared_mutex> write_lock{*map_mutex, std::try_to_lock};
        if (write_lock.owns_lock()) {
            // got write lock
            _add(tid, eid, xid, rows, map, write_lock);
            return;
        }

        // didn't get the write lock, get read lock, check
        std::shared_lock<std::shared_mutex> map_read_lock{*map_mutex};
        auto itr = map->find(tid);
        if (itr != map->end()) {
            // found existing entry in top level map; add to existing entry
            EntryPtr entry = itr->second;
            _add_existing(entry, eid, xid, rows);
            return;
        }
        // unlock the read lock, need to lock exclusive
        map_read_lock.unlock();

        // not found in top level map, get write lock at top level and try again...
        std::unique_lock<std::shared_mutex> map_write_lock{*map_mutex};
        _add(tid, eid, xid, rows, map, map_write_lock);

        return;
    }

    /** Remove an entry for an tid for a given xid range; exclusive of start */
    void
    WriteCacheIndexRowMap::remove(uint64_t tid, uint64_t eid,
                                  uint64_t start_xid, uint64_t end_xid)
    {
        std::shared_ptr<std::shared_mutex> map_mutex = _get_mutex(tid);
        std::shared_ptr<std::map<uint64_t, EntryPtr>> map = _get_map(tid);

        // lock top level map and search for id
        std::shared_lock<std::shared_mutex> read_lock{*map_mutex};
        auto itr = map->find(tid);
        if (itr == map->end()) {
            // not found
            return;
        }

        // found, unlock top level map
        EntryPtr entry = itr->second;

        // lock entry map and search for row
        WriteCacheIndexRowPtr rkey = std::make_shared<WriteCacheIndexRow>(eid, start_xid+1);

        std::unique_lock<std::shared_mutex> entry_lock{entry->entry_mutex};
        auto start_itr = entry->set.lower_bound(rkey);
        if (start_itr == entry->set.end()) {
            // not found
            return;
        }

        // start at start_itr and move forward to find end of xid range
        auto end_itr = start_itr;
        while (end_itr != entry->set.end() && (*end_itr)->xid <= end_xid) {
            end_itr++;
        }

        // found, erase entry and check if map is empty
        entry->set.erase(start_itr, end_itr);
        if (!entry->set.empty()) {
            return;
        }
        // entry is empty
        entry_lock.unlock();
        read_lock.unlock();

        // if map is empty, write lock top level map
        std::unique_lock<std::shared_mutex> write_lock{*map_mutex};
        itr = map->find(tid);
        if (itr == map->end()) {
            // not found
            return;
        }

        // write lock entry and check again that it is empty; if so remove entry
        entry = itr->second;
        std::unique_lock<std::shared_mutex> entry_write_lock{entry->entry_mutex};
        if (entry->set.empty()) {
            map->erase(itr);
        }

        return;
    }

    /** Retreive a set of rows for an xid range, return number of rows */
    int
    WriteCacheIndexRowMap::get(uint64_t tid, uint64_t eid, uint64_t start_xid,
                               uint64_t end_xid, uint32_t count, uint64_t &cursor,
                               std::vector<std::shared_ptr<WriteCacheIndexRow>> &result) const
    {
        std::shared_ptr<std::shared_mutex> map_mutex = _get_mutex(tid);
        std::shared_ptr<std::map<uint64_t, EntryPtr>> map = _get_map(tid);

        // lock table map
        std::shared_lock<std::shared_mutex> map_lock{*map_mutex};
        auto itr = map->find(tid);
        if (itr == map->end()) {
            // nothing found
            return 0;
        }
        // found entry
        EntryPtr entry = itr->second;

        // lookup row; exclusive of start
        WriteCacheIndexRowPtr rkey = std::make_shared<WriteCacheIndexRow>(eid, start_xid+1);
        std::shared_lock<std::shared_mutex> entry_lock{entry->entry_mutex};
        auto entry_itr = entry->set.lower_bound(rkey);

        // iterate through rows to find those within the xid range
        SPDLOG_DEBUG("Row get: TID:{} EID:{} XIDs:{}:{}\n", tid, eid, start_xid+1, end_xid);
        int added = 0;
        uint64_t offset = cursor;
        while (entry_itr != entry->set.end() && (*entry_itr)->eid == eid &&
                added < count && (*entry_itr)->xid <= end_xid)
        {
            // check cursor offset, if > 0 decr until we get to start of cursor
            if (offset > 0) {
                offset--;
                entry_itr++;
                continue;
            }

            // incr cursor since we are beyond its starting point
            cursor++;

            // add element
            result.push_back(*entry_itr);
            added++;
            entry_itr++;
        }

        return added;
    }

    /** Helper, to add an index node at an xid for an ID; assumes a write lock exists on the external map */
    void
    WriteCacheIndexRowMap::_add(uint64_t tid, uint64_t eid, uint64_t xid,
                                const std::vector<WriteCacheIndexRowPtr> &rows,
                                std::shared_ptr<std::map<uint64_t, EntryPtr>> map,
                                std::unique_lock<std::shared_mutex> &map_lock)
    {
        // write lock is held on map mutex
        auto itr = map->find(tid);
        if (itr != map->end()) {
            // entry found in outer map
            EntryPtr entry = itr->second;

            // could downgrade to a map read lock if it were possible...
            // this shouldn't be common case though, or if it is there
            // shouldn't have been contention on the map write lock

            // add to the existing node
            _add_existing(entry, eid, xid, rows);
            return;
        }

        // map write lock is held
        // no entry found in outer map, create an entry and update that entry's map
        EntryPtr entry = std::make_shared<Entry>();
        for (int i = 0; i < rows.size(); i++) {
            entry->set.insert(rows[i]);
        }
        map->insert({tid, entry});
        return;
    }

    /**
     * Helper to replace an existing row's data if it exists. Need to check xid_seq.
     * A map lock should be held, either read or write
     */
    void
    WriteCacheIndexRowMap::_add_existing(EntryPtr entry, uint64_t eid, uint64_t xid,
                                         const std::vector<WriteCacheIndexRowPtr> &rows)
    {
        // add entry if it doesn't exist
        // currently we store multiple copies per xid
        // in future we can merge or remove old entries based on type of op
        std::unique_lock<std::shared_mutex> entry_lock{entry->entry_mutex};
        for (int i = 0; i < rows.size(); i++) {
            // XXX need to check if row already exists in the set, if so just replace it
            entry->set.insert(rows[i]);
        }
    }

}