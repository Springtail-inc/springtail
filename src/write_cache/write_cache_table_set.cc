#include <vector>
#include <memory>
#include <mutex>
#include <cassert>
#include <iostream>

#include <xxhash.h>
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
        // get the rid for the rows
        std::vector<std::string> rids;

        uint64_t xid = data[0]->xid;

        // generate set of rids for each row
        for (auto r: data) {
            std::string rid = _get_row_key(r->pkey);
            rids.push_back(rid);
            assert(r->xid == xid);
        }

        // add row into row data -- note: an emplace may be done on data, so not safe to access after this.
        _row_map->add(tid, eid, xid, rids, data);

        // update the xid map to hold the row metadata (excluding the actual row data)
        _insert_rows(tid, eid, xid, rids);
    }


    void
    WriteCacheTableSet::_insert_rows(uint64_t tid, uint64_t eid, uint64_t xid,
                                     const std::vector<std::string> &rids)
    {
        // find the xid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr xid_node = _xid_root->findAdd(xid, WriteCacheIndexNode::IndexType::XID);

        // find the tid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr tid_node = xid_node->findAdd(tid, WriteCacheIndexNode::IndexType::TABLE);

        // find the eid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr eid_node = tid_node->findAdd(tid, WriteCacheIndexNode::IndexType::EXTENT);

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
            // fetch xid node for this xid and read lock it
            WriteCacheIndexNodePtr xid_node = _xid_root->find(xid);
            if (xid_node == nullptr) {
                continue;
            }

            // fetch ids into set to keep them unique
            result_cnt += _fetch_ids(xid_node, count-result_cnt, set);
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

        auto key = std::make_shared<WriteCacheIndexTableChange>(tid, start_xid, 0);
        auto itr = _table_change_set.lower_bound(key);
        while (itr != _table_change_set.end() && (*itr)->xid <= end_xid) {
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


    std::string
    WriteCacheTableSet::_get_row_key(const std::string &pkey)
    {
        if (pkey.length() < 32) {
            return pkey;
        }

        XXH128_hash_t hash = XXH3_128bits(pkey.c_str(), pkey.length());
        return fmt::format("{X}{X}", hash.high64, hash.low64);
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

static int s_id=0;

void
add_row(springtail::WriteCacheTableSet &ts, uint64_t tid, uint64_t eid, uint64_t xid, int rid=s_id)
{
    springtail::WriteCacheIndexRowPtr row =
        std::make_shared<springtail::WriteCacheIndexRow>("data", fmt::format("key:{}", rid), xid, s_id);

    std::cout << fmt::format("\nInserting row: tid: {}, eid: {}, rid: {}, xid: {}, xid_seq: {}, key:{}\n",
                             tid, eid, rid, xid, s_id, rid);

    if (rid == s_id) {
        s_id++;
    }

    std::vector<springtail::WriteCacheIndexRowPtr> rows;
    rows.push_back(row);
    ts.add_rows(tid, eid, rows);
}

std::string
dump_row_data(springtail::WriteCacheIndexRowPtr row)
{
    return fmt::format("XID: {}:{} RID: {}", row->xid, row->xid_seq, row->pkey);
}

void
dump_rows(const std::vector<springtail::WriteCacheIndexRowPtr> rows)
{
    std::cout << "Dumping row data\n";
    for (auto r: rows) {
        std::cout << dump_row_data(r) << std::endl;
    }
}

int main(void)
{
    springtail::springtail_init();

    springtail::WriteCacheTableSet ts;

    add_row(ts, 1, 1, 5); // tid, eid, xid, rid=s_id
    add_row(ts, 1, 1, 6);
    add_row(ts, 1, 1, 9);
    add_row(ts, 1, 1, 7);
    add_row(ts, 1, 1, 10);

    ts.dump();

    std::vector<springtail::WriteCacheIndexRowPtr> rows;

    ts.get_rows(1, 1, 6, 9, 10, rows);
    dump_rows(rows);
    rows = {};

    add_row(ts, 1, 2, 8);
    add_row(ts, 1, 2, 7);
    add_row(ts, 1, 2, 15);

    ts.dump();

    ts.get_rows(1, 1, 6, 9, 10, rows);
    dump_rows(rows);
    rows = {};

    // update key:2 with xid 10
    add_row(ts, 1, 1, 10, 2);

    ts.dump();

    std::cout << "Allocated bytes: " << springtail::TrackingAllocatorStats::get_instance()->get_allocated_bytes() << std::endl;

    std::cout << "Evicting extent: tid=1, eid=2, xids: 8:15\n";
    ts.evict_table(1, 8, 15);

    ts.dump();

    std::cout << "Evicting extent: tid=1, eid=1, xids: 5:7\n";
    ts.evict_table(1, 5, 7);

    ts.dump();

    std::cout << "Allocated bytes: " << springtail::TrackingAllocatorStats::get_instance()->get_allocated_bytes() << std::endl;

    return 0;
}