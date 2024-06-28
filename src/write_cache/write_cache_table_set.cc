#include <vector>
#include <memory>
#include <mutex>
#include <cassert>
#include <iostream>

#include <fmt/core.h>

#include <common/common.hh>
#include <common/logging.hh>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail
{

    WriteCacheTableSet::WriteCacheTableSet(int row_table_partitions) :
        _xid_root(std::make_shared<WriteCacheIndexNode>(-1, WriteCacheIndexNode::IndexType::ROOT)),
        _row_map(std::make_shared<WriteCacheIndexRowMap>(row_table_partitions))
    {}

    void
    WriteCacheTableSet::add_rows(uint64_t tid, uint64_t eid,
                                 const std::vector<WriteCacheIndexRowPtr> &data)
    {
        // get xid from first element, should all be the same
        uint64_t xid = data[0]->xid;

        // add row into row data -- note: an emplace may be done on data, so not safe to access after this.
        _row_map->add(tid, eid, xid, data);

        // update the xid map to hold the row metadata (excluding the actual row data)
        _insert_rows(tid, eid, xid);
    }


    void
    WriteCacheTableSet::_insert_rows(uint64_t tid, uint64_t eid, uint64_t xid)
    {
         SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Inserting: {}:{}:{}\n", tid, eid, xid);

        // find the xid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr xid_node = _xid_root->findAdd(xid, WriteCacheIndexNode::IndexType::XID);

        // find the tid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr tid_node = xid_node->findAdd(tid, WriteCacheIndexNode::IndexType::TABLE);

        // find the eid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr eid_node = tid_node->findAdd(eid, WriteCacheIndexNode::IndexType::EXTENT);

        // NOTE: rows are not stored here, they are stored in the _row_map only
    }

    int
    WriteCacheTableSet::get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid,
                                 uint64_t end_xid, uint32_t count, uint64_t &cursor,
                                 std::vector<std::shared_ptr<WriteCacheIndexRow>> &result)
    {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Searching for rows in range: {}:{}\n", start_xid, end_xid);
        return _row_map->get(tid, eid, start_xid, end_xid, count, cursor, result);
    }

    int
    WriteCacheTableSet::get_tids(uint64_t start_xid, uint64_t end_xid,
                                 uint32_t count, uint64_t start_offset, uint64_t &end_offset,
                                 std::vector<int64_t> &result)
    {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Searching for tids in range: {}:{}\n", start_xid, end_xid);

        int result_cnt = 0;
        std::set<uint64_t> set;
        end_offset = 0;

        // iterate through xids exclusive of start
        for (uint64_t xid = start_xid + 1; xid <= end_xid && result_cnt < count; xid++) {
             SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Finding tids in xid: {}\n", xid);
            // fetch xid node for this xid and read lock it
            WriteCacheIndexNodePtr xid_node = _xid_root->find(xid);
            if (xid_node == nullptr) {
                SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "XID {} not found\n", xid);
                continue;
            }

            // fetch ids into set to keep them unique
            result_cnt += _fetch_ids(xid_node, count-result_cnt, true, start_offset, end_offset, set);
            SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Found unique tids, result_cnt={}\n", result_cnt);
        }

        // copy results to vector
        for (auto i: set) {
            result.push_back(i);
        }

        return result_cnt;
    }

    int
    WriteCacheTableSet::get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                                 uint32_t count, uint64_t &cursor, std::vector<int64_t> &result)
    {
        int result_cnt = 0;
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Searching for eids in range: {}:{}\n", start_xid, end_xid);

        std::set<uint64_t> set;
        uint64_t start_offset = cursor;
        uint64_t end_offset = 0;

        // iterate through xids exclusive of start
        for (uint64_t xid = start_xid + 1; xid <= end_xid && result_cnt < count; xid++) {
            // fetch xid node for this xid if exists
            WriteCacheIndexNodePtr xid_node = _xid_root->find(xid);
            if (xid_node == nullptr) {
                SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "XID not found: {}", xid);
                continue;
            }

            // fetch tid node if exists
            WriteCacheIndexNodePtr tid_node = xid_node->find(tid);
            if (tid_node == nullptr) {
                SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "TID not found: {}", xid);
                continue;
            }

            SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Fetching eids for TID={}, XID={}, end_offset={}\n", tid, xid, end_offset);

            // fetch ids into set to keep them unique; start_offset is decr; end_offset is incr
            result_cnt += _fetch_ids(tid_node, count - result_cnt, true, start_offset, end_offset, set);
        }

        assert(end_offset >= cursor);
        cursor = end_offset;

        // copy results to vector
        for (auto i: set) {
            result.push_back(i);
        }

        return result_cnt;
    }

    int
    WriteCacheTableSet::_fetch_ids(WriteCacheIndexNodePtr node, uint32_t count,
                                   bool skip_clean, uint64_t &start_offset,
                                   uint64_t &end_offset, std::set<uint64_t> &result)
    {
        int result_cnt = 0;

        // read lock it and iterate through eids
        std::shared_lock<std::shared_mutex> read_lock{node->mutex};

        // iterate through children adding to result set
        auto itr = node->children.begin();
        while (itr != node->children.end()) {
            end_offset++;

            // check cursor offset, decr if above 0 and continue
            if (start_offset > 0) {
                start_offset--;
                itr++;
                continue;
            }

            // skip clean entries
            if (skip_clean && (*itr)->is_clean) {
                itr++;
                continue;
            }

            auto res = result.insert((*itr)->id);
            // see whether we had this item in the set after the insert is done
            if (res.second) {
                if (++result_cnt == count) {
                    return result_cnt;
                }
            }
            itr++;
        }

        return result_cnt;
    }

    void
    WriteCacheTableSet::evict_table(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        std::set<uint64_t> eid_set;

        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Evicting table: TID={} XIDs=({}:{}]\n", tid, start_xid, end_xid);

        // iterate through xids exclusive of start
        for (uint64_t xid = start_xid + 1; xid <= end_xid; xid++) {
            // fetch xid node for this xid if exists
            SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Searching for XID: {}\n", xid);
            WriteCacheIndexNodePtr xid_node = _xid_root->find(xid);
            if (xid_node == nullptr) {
                 SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "XID {} not found\n", xid);
                continue;
            }

            SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Removing TID: {}\n", tid);
            WriteCacheIndexNodePtr table_node = xid_node->remove(tid);
            if (table_node != nullptr) {
                // add eid to extent set, so we can remove row data later
                for (auto extent_node: table_node->children) {
                    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Found table {}, adding extent eid={}\n", tid, extent_node->id);
                    eid_set.insert(extent_node->id);
                }
            }
        }

        // remove row data for table for each extent and xid range
        // if this set gets too big, may have to do partial removals inline in the loop above
        for (auto eid: eid_set) {
            _row_map->remove(tid, eid, start_xid, end_xid);
        }
    }

    void
    WriteCacheTableSet::add_table_change(WriteCacheIndexTableChangePtr change)
    {
        std::unique_lock lock{_table_change_mutex};
        _table_change_set.insert(change);
    }

    void
    WriteCacheTableSet::get_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                                          std::vector<std::shared_ptr<WriteCacheIndexTableChange>> &changes)
    {
        std::shared_lock lock{_table_change_mutex};

        auto key = std::make_shared<WriteCacheIndexTableChange>(tid, start_xid+1, 0);
        auto itr = _table_change_set.lower_bound(key);
        while (itr != _table_change_set.end() && (*itr)->xid <= end_xid && (*itr)->tid == tid) {
            changes.push_back((*itr));
            itr++;
        }
    }

    void // done
    WriteCacheTableSet::evict_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        std::unique_lock lock{_table_change_mutex};

        auto key = std::make_shared<WriteCacheIndexTableChange>(tid, start_xid+1, 0);
        auto start_itr = _table_change_set.lower_bound(key); // exclusive of start
        auto end_itr = start_itr; // find end_itr, should be one more than actual end
        while (end_itr != _table_change_set.end() &&
              (*end_itr)->xid <= end_xid && (*end_itr)->tid == tid) {
            end_itr++;
        }

        // remove elements [start_itr, end_itr)
        _table_change_set.erase(start_itr, end_itr);
    }

    void
    WriteCacheTableSet::set_clean_flag(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid)
    {
        // iterate through xids exclusive of start
        for (uint64_t xid = start_xid + 1; xid <= end_xid; xid++) {
            // fetch xid node for this xid if exists
            WriteCacheIndexNodePtr xid_node = _xid_root->find(xid);
            if (xid_node == nullptr) {
                continue;
            }

            // fetch tid node if exists
            WriteCacheIndexNodePtr tid_node = xid_node->find(tid);
            if (tid_node == nullptr) {
                continue;
            }

            // fetch eid node if exists
            WriteCacheIndexNodePtr eid_node = tid_node->find(eid);
            if (eid_node == nullptr) {
                continue;
            }

            // write lock it and update the flag
            std::unique_lock<std::shared_mutex> write_lock{eid_node->mutex};
            eid_node->is_clean = true;
            write_lock.unlock();
        }
    }

    void
    WriteCacheTableSet::reset_clean_flag(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        // iterate through xids exclusive of start
        for (uint64_t xid = start_xid + 1; xid <= end_xid; xid++) {
            // fetch xid node for this xid if exists
            WriteCacheIndexNodePtr xid_node = _xid_root->find(xid);
            if (xid_node == nullptr) {
                continue;
            }

            // fetch tid node if exists
            WriteCacheIndexNodePtr tid_node = xid_node->find(tid);
            if (tid_node == nullptr) {
                continue;
            }

            // read lock table node while iterating children
            std::shared_lock<std::shared_mutex> read_lock{tid_node->mutex};
            for (auto c: tid_node->children) {
                // write lock extent node
                std::unique_lock<std::shared_mutex> write_lock{c->mutex};
                c->is_clean = false;
                write_lock.unlock();
            }
            read_lock.unlock();
        }
    }

    void
    WriteCacheTableSet::dump()
    {
        std::cout << "\nDumping table\n";
        _dump(_xid_root);
        std::cout << std::endl;

        std::cout << "Dumping Row Data Map\n";
        _row_map->dump();
        std::cout << std::endl;
    }

    void
    WriteCacheTableSet::_dump(WriteCacheIndexNodePtr node)
    {
        if (node->type == springtail::WriteCacheIndexNode::IndexType::XID) {
            std::cout << std::endl;
        }
        std::cout << node->dump();
        for (auto x = node->children.begin(); x != node->children.end(); x++) {
            _dump(*x);
        }
    }
}
