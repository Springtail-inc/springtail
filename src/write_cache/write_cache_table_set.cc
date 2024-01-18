#include <vector>
#include <memory>
#include <mutex>
#include <cassert>
#include <iostream>

#include <fmt/core.h>

#include <common/common.hh>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail
{

    WriteCacheTableSet::WriteCacheTableSet() :
        _xid_root(std::make_shared<WriteCacheIndexNode>(-1, WriteCacheIndexNode::IndexType::ROOT)),
        _row_map(std::make_shared<WriteCacheIndexRowMap>())
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
        std::cout << fmt::format("Inserting: {}:{}:{}\n", tid, eid, xid);

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
                                 uint64_t end_xid, int count,
                                 std::vector<std::shared_ptr<WriteCacheIndexRow>> &result)
    {
        std::cout << "Searching for rows in range: " << start_xid << ":" << end_xid << std::endl;
        return _row_map->get(tid, eid, start_xid, end_xid, count, result);
    }

    int
    WriteCacheTableSet::get_tids(uint64_t start_xid, uint64_t end_xid,
                                 int count, std::vector<int64_t> &result)
    {
        int result_cnt = 0;
        std::cout << "Searching for tids in range: (" << start_xid << ":" << end_xid << "]\n";

        std::set<uint64_t> set;

        // iterate through xids exclusive of start
        for (uint64_t xid = start_xid + 1; xid <= end_xid && result_cnt < count; xid++) {
            std::cout << "Finding xid: " << xid << std::endl;
            // fetch xid node for this xid and read lock it
            WriteCacheIndexNodePtr xid_node = _xid_root->find(xid);
            if (xid_node == nullptr) {
                std::cout << " - not found\n";
                continue;
            }

            // fetch ids into set to keep them unique
            result_cnt += _fetch_ids(xid_node, count-result_cnt, set);
            std::cout << "Found ids, result_cnt=" << result_cnt << std::endl;
        }

        // copy results to vector
        for (auto i: set) {
            result.push_back(i);
        }

        return result_cnt;
    }

    int
    WriteCacheTableSet::get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                                 int count, std::vector<int64_t> &result)
    {
        int result_cnt = 0;
        std::cout << "Searching for eids in range: " << start_xid << ":" << end_xid << std::endl;

        std::set<uint64_t> set;

        // iterate through xids exclusive of start
        for (uint64_t xid = start_xid + 1; xid <= end_xid && result_cnt < count; xid++) {
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

            // fetch ids into set to keep them unique
            result_cnt += _fetch_ids(tid_node, count - result_cnt, set);
        }

        // copy results to vector
        for (auto i: set) {
            result.push_back(i);
        }

        return result_cnt;
    }

    int
    WriteCacheTableSet::_fetch_ids(WriteCacheIndexNodePtr node, int count, std::set<uint64_t> &result)
    {
        int result_cnt = 0;

        // read lock it and iterate through eids
        std::shared_lock<std::shared_mutex> read_lock{node->mutex};

        // iterate through children adding to result set
        auto itr = node->children.begin();
        while (itr != node->children.end()) {
            auto res = result.insert((*itr)->id);
            if (res.second) {
                result_cnt++;
            }
            if (result_cnt == count) {
                return result_cnt;
            }
            itr++;
        }

        return result_cnt;
    }

    void
    WriteCacheTableSet::evict_table(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        std::set<uint64_t> eid_set;

        // iterate through xids exclusive of start
        for (uint64_t xid = start_xid + 1; xid <= end_xid; xid++) {
            // fetch xid node for this xid if exists
            WriteCacheIndexNodePtr xid_node = _xid_root->find(xid);
            if (xid_node == nullptr) {
                continue;
            }

            WriteCacheIndexNodePtr table_node = xid_node->remove(tid);
            if (table_node != nullptr) {
                // add eid to extent set, so we can remove row data later
                for (auto extent_node: table_node->children) {
                    eid_set.insert(extent_node->id);
                }
            }
        }

        // remove row data for table for each extent and xid range
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

        key = std::make_shared<WriteCacheIndexTableChange>(tid, end_xid, 0);
        auto end_itr = _table_change_set.upper_bound(key);  // inclusive of end

        // remove elements [start_itr, end_itr)
        _table_change_set.erase(start_itr, end_itr);
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
        std::cout << node->dump();
        for (auto x = node->children.begin(); x != node->children.end(); x++) {
            _dump(*x);
        }
    }
}