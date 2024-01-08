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
        _table_root(std::make_shared<XidIdRangeSet_t>())
    {}

    void
    WriteCacheTableSet::add_row(uint64_t tid, uint64_t eid, std::shared_ptr<WriteCacheIndexRow> data)
    {
        // get the rid for the row
        rid_t rid = _get_rid_from_row(data->pkey);

        uint64_t xid = data->xid;

        // add row into row data -- note: an emplace may be done on data, so not safe to access after this.
        if (_insert_row_data(tid, eid, rid, data)) {
            // data already existed at the xid, so no need to add to additional maps
            return;
        }

        // need to add to EID map
        // lookup xid range struct in eid map and update
        // if EID doesn't exist then also check table map
        _insert_extent_index(tid, eid, rid, xid);
    }

    int
    WriteCacheTableSet::get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid,
                                 uint64_t end_xid, int count,
                                 std::vector<std::shared_ptr<WriteCacheIndexRow>> &result)
    {
        std::cout << "Searching for rows in range: " << start_xid << ":" << end_xid << std::endl;

        std::string eid_key = _get_extent_key(tid, eid);

        // lookup eid in eid map
        auto eid_itr = _eid_map.find(eid_key);
        if (eid_itr == _eid_map.end()) {
            // nothing here
            return 0;
        }

        assert(eid_itr->second->type == XidIdRange::IndexType::EXTENT);
        assert(eid_itr->second->get_id() == eid);

        int result_cnt = 0;

        // create a search entry and search in eid xid range child set
        std::shared_ptr<XidIdRange> search_range = std::make_shared<XidIdRange>(start_xid, end_xid);
        auto search_itr = eid_itr->second->children->lower_bound(search_range);

        while (search_itr != eid_itr->second->children->end() &&
               (*search_itr)->start_xid <= end_xid &&
               result_cnt < count)
        {
            assert((*search_itr)->p_id == eid);
            std::cout << "Row ID: " << (*search_itr)->get_rid()
                      << ", Xid range: " << (*search_itr)->start_xid << ":"
                      << (*search_itr)->end_xid << std::endl;

            // lookup row id in row data set
            RowDataKey rkey(tid, eid, (*search_itr)->start_xid, (*search_itr)->get_rid());
            auto r_itr = _row_data_map.find(rkey);
            assert(r_itr != _row_data_map.end());

            // add row data to result vector
            result.push_back(r_itr->second);
            result_cnt++;

            search_itr++;
        }

        return result_cnt;
    }

    int
    WriteCacheTableSet::get_tids(uint64_t start_xid, uint64_t end_xid,
                                 int count, std::vector<int64_t> &result)
    {
        int result_cnt = 0;

        std::cout << "Searching for tids in range: " << start_xid << ":" << end_xid << std::endl;

        // find lower bound: first element in range [start_xid, end_xid)
        std::shared_ptr<XidIdRange> search_range = std::make_shared<XidIdRange>(start_xid, end_xid);
        auto search_itr = _table_root->lower_bound(search_range);

        // iterate through table root set of tids, returning at most count tids
        while (search_itr != _table_root->end() &&
               (*search_itr)->start_xid <= end_xid &&
               result_cnt < count)
        {
            assert((*search_itr)->p_id == -1);
            std::cout << "TID: " << (*search_itr)->get_id()
                      << ", Xid range: " << (*search_itr)->start_xid << ":"
                      << (*search_itr)->end_xid << std::endl;

            // add row data to result vector
            result.push_back((*search_itr)->get_id());
            result_cnt++;

            search_itr++;
        }

        return result_cnt;
    }

    int
    WriteCacheTableSet::get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                                 int count, std::vector<int64_t> &result)
    {
        int result_cnt = 0;

        std::cout << "Searching for eids in range: " << start_xid << ":" << end_xid << std::endl;

        // lookup eid in eid map
        auto tid_itr = _tid_map.find(tid);
        if (tid_itr == _tid_map.end()) {
            // nothing here
            return 0;
        }

        assert(tid_itr->second->type == XidIdRange::IndexType::TABLE);
        assert(tid_itr->second->get_id() == tid);

        // create a search entry and search in eid xid range child set
        std::shared_ptr<XidIdRange> search_range = std::make_shared<XidIdRange>(start_xid, end_xid);
        // find lower bound: first element in range [start_xid, end_xid)
        auto search_itr = tid_itr->second->children->lower_bound(search_range);

        // iterate through table children (extents), returning at most count eids
        while (search_itr != tid_itr->second->children->end() &&
               (*search_itr)->start_xid <= end_xid &&
               result_cnt < count)
        {
            assert((*search_itr)->p_id == tid);

            std::cout << "Extent ID: " << (*search_itr)->get_id()
                      << ", Xid range: " << (*search_itr)->start_xid << ":"
                      << (*search_itr)->end_xid << std::endl;

            // add row data to result vector
            result.push_back((*search_itr)->get_id());
            result_cnt++;

            search_itr++;
        }

        return result_cnt;
    }

    void
    WriteCacheTableSet::evict_extent(uint64_t tid, uint64_t eid,
                                     uint64_t start_xid, uint64_t end_xid)
    {
        // lookup extent in map
        std::string key = _get_extent_key(tid, eid);
        auto eid_itr = _eid_map.find(key);
        if (eid_itr == _eid_map.end()) {
            std::cout << fmt::format("No extent found for eviction: tid={}, eid={}\n", tid, eid);
            return;
        }

        // find xid range node for extent; children is a set of rows
        auto extent_node = eid_itr->second;

        assert(extent_node->type == XidIdRange::IndexType::EXTENT);
        assert(extent_node->get_id() == eid);
        assert(extent_node->p_id == tid);

        // find the lower and upper bound based on start and end xid
        // set end_xid in 0 for the comparator to return proper results
        std::shared_ptr<XidIdRange> search_range = std::make_shared<XidIdRange>(start_xid, 0);
        auto start_itr = extent_node->children->lower_bound(search_range);

        if (start_itr == extent_node->children->end()) {
            std::cout << fmt::format("No child row found for xid range: {}:{}\n", start_xid, end_xid);
            return;
        }

        // reset search_range with start_xid pointing to the end_xid
        search_range->start_xid = end_xid;
        auto end_itr = extent_node->children->upper_bound(search_range);

        std::cout << "Start itr\n";
        (*start_itr)->dump();

        std::cout << "End itr\n";
        (*end_itr)->dump();

        std::cout << "Dumping matching start end range\n";
        for (auto i = start_itr; i != end_itr; i++) {
            (*i)->dump();
        }

        // remove rows from eid set
        auto ssize = extent_node->children->size();
        extent_node->children->erase(start_itr, end_itr);
        std::cout << fmt::format("Removed: {} items from children set\n",
                                 ssize-extent_node->children->size());

        // if all rows have been removed, remove from _eid_map
        if (extent_node->children->empty()) {
            // remove extent from extent map
            _eid_map.erase(eid_itr);

            std::cout << "Removed all children from extent\n";

            // need to remove from parent children
            auto tid_itr = _tid_map.find(tid);
            assert(tid_itr != _tid_map.end());
            auto table_node = tid_itr->second;

            // find eid in tid children
            auto child_itr = table_node->children->find(extent_node);
            assert(child_itr != table_node->children->end());
            assert((*child_itr)->get_id() == eid);

            // remove eid from children
            table_node->children->erase(child_itr);

        } else {
            std::cout << "Partial removal from extent, need fixup\n";

            // need to fixup start_xid, end_xid range; find min/max xid from children
            auto child_itr = extent_node->children->begin();
            uint64_t min_xid = (*child_itr)->start_xid;
            auto rchild_itr = extent_node->children->rbegin();
            uint64_t max_xid = (*rchild_itr)->end_xid;

            std::cout << fmt::format("Found min xid={}, max xid={}\n", min_xid, max_xid);
            std::cout << fmt::format("Child node xid range: {}:{}\n",
                                     extent_node->start_xid, extent_node->end_xid);

            _fixup_eid_range(tid, eid, start_xid, end_xid, extent_node, false);
        }
    }

    void
    WriteCacheTableSet::add_table_change(std::shared_ptr<WriteCacheIndexTableChange> change)
    {
        _table_change_set.insert(change);
    }

    void
    WriteCacheTableSet::get_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                                          std::vector<std::shared_ptr<WriteCacheIndexTableChange>> &changes)
    {
        auto key = std::make_shared<WriteCacheIndexTableChange>(tid, start_xid, 0);

        auto itr = _table_change_set.lower_bound(key);
        while (itr != _table_change_set.end() && (*itr)->xid <= end_xid) {
            changes.push_back((*itr));
            itr++;
        }
    }

    void
    WriteCacheTableSet::evict_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        auto key = std::make_shared<WriteCacheIndexTableChange>(tid, start_xid, 0);
        auto start_itr = _table_change_set.lower_bound(key);

        key = std::make_shared<WriteCacheIndexTableChange>(tid, end_xid, 0);
        auto end_itr = _table_change_set.upper_bound(key);

        // remove elements [start_itr, end_itr)
        _table_change_set.erase(start_itr, end_itr);
    }

    void
    WriteCacheTableSet::_remove_row_data(uint64_t tid, uint64_t eid,
                                         uint64_t start_xid, uint64_t end_xid)
    {
        std::unique_lock lock{_row_data_mutex};

        RowDataKey start_key(tid, eid, start_xid);
        RowDataKey end_key(tid, eid, end_xid);

        auto rid_start_itr = _row_data_map.lower_bound(start_key);
        auto rid_end_itr = _row_data_map.upper_bound(end_key);

        if (rid_start_itr == _row_data_map.end()) {
            return;
        }
        // remove data from row map
        _row_data_map.erase(rid_start_itr, rid_end_itr);
    }

    bool
    WriteCacheTableSet::_insert_row_data(uint64_t tid, uint64_t eid, rid_t rid,
                                         std::shared_ptr<WriteCacheIndexRow> data)
    {
        // generate key
        RowDataKey key(tid, eid, data->xid, rid);

        std::unique_lock lock{_row_data_mutex};

        auto p = _row_data_map.insert_or_assign(key, data);
        if (!p.second) {
            // an entry already exists, so new data was not inserted
            // p.first holds node (pair<key,value>)
            // check xid seq of existing element to see if an update is needed
            // if so, then check the xid ranges to see if they need updating
            assert(p.first->second->xid == data->xid);

            std::cout << "Insert row found entry\n";

            if (p.first->second->xid_seq < data->xid_seq) {
                _row_data_map.emplace_hint(p.first, key, data);
            }

            return true;
        }
        std::cout << "Insert row no entry found\n";

        return false; // no data existed
    }

    void
    WriteCacheTableSet::_insert_table_index(uint64_t tid, uint64_t xid,
                                            std::shared_ptr<XidIdRange> eid_xid_range)
    {
        // check to see if table entry exists, if not insert that as well
        auto tid_itr = _tid_map.find(tid);
        if (tid_itr == _tid_map.end()) {
            // entry not found create tid entry
            std::shared_ptr<XidIdRange> tid_xid_range =
                std::make_shared<XidIdRange>(xid, xid, XidIdRange::IndexType::TABLE, tid);

            // add eid xid range to tid children set
            tid_xid_range->children->insert(eid_xid_range);

            // add tid range to the root
            _table_root->insert(tid_xid_range);

            // update the tid map
            _tid_map.emplace(tid, tid_xid_range);
        } else {
            // found tid entry, add eid_xid_range to tid children
            tid_itr->second->children->insert(eid_xid_range);

            // see if we need to fixup xid range
            if (tid_itr->second->start_xid > xid || tid_itr->second->end_xid < xid) {
                std::cout << "TID fixup on insert\n";
                _fixup_set(tid, xid, xid, tid_itr->second, _table_root, true);
            }
        }
    }

    void
    WriteCacheTableSet::_insert_extent_index(uint64_t tid, uint64_t eid,
                                             rid_t rid, uint64_t xid)
    {
        std::string key = _get_extent_key(tid, eid);
        auto eid_itr = _eid_map.find(key);

        // create row entry
        std::shared_ptr<XidIdRange> row_node = std::make_shared<XidIdRange>(xid, rid, eid);

        if (eid_itr == _eid_map.end()) {
            // entry not found, create entry
            // create eid range
            std::shared_ptr<XidIdRange> extent_node =
                std::make_shared<XidIdRange>(xid, xid, XidIdRange::IndexType::EXTENT, eid, tid);
            // add row entry to extent children set
            extent_node->children->insert(row_node);

            // add new eid_xid range to the tid map
            _insert_table_index(tid, xid, extent_node);

            // add eid range to eid_map
            _eid_map.emplace(key, extent_node);
        } else {
            // extent found in eid map
            std::shared_ptr<XidIdRange> extent_node = eid_itr->second;

            std::cout << "Found eid, adding row\n";

            // add row range to eid children
            extent_node->children->insert(row_node);

            // check eid and tid ranges to see if they need to be fixed up based on new xid
            _fixup_eid_range(tid, eid, xid, xid, extent_node, true);
        }
    }

    bool
    WriteCacheTableSet::_check_start_range(std::shared_ptr<XidIdRange> node,
                                             uint64_t start_xid, uint64_t end_xid,
                                             bool expand_range)
    {
        /*
         * we are inserting a range, if so then check if the new xids are
         * outside the existing range.  Result is to expand existing range.
         */
        if (expand_range && (node->start_xid > start_xid)) {
            return true;
        }

        /*
         * we are removing a range, if so then check if the new xids overlap
         * with the existing range.  Result is to shrink existing range.
         */
        if (!expand_range && (node->start_xid >= start_xid && node->start_xid < end_xid)) {
            return true;
        }

        return false;
    }

    bool
    WriteCacheTableSet::_check_end_range(std::shared_ptr<XidIdRange> node,
                                           uint64_t start_xid, uint64_t end_xid,
                                           bool expand_range)
    {
        /*
         * we are inserting a range, if so then check if the new xids are
         * outside the existing range.  Result is to expand existing range.
         */
        if (expand_range && (node->end_xid < end_xid)) {
            return true;
        }

        /*
         * we are removing a range, if so then check if the new xids overlap
         * with the existing range.  Result is to shrink existing range.
         */
        if (!expand_range && (node->end_xid >= start_xid && node->end_xid < end_xid)) {
            return true;
        }

        return false;
    }

    void
    WriteCacheTableSet::_fixup_eid_range(uint64_t tid, uint64_t eid,
                                         uint64_t start_xid, uint64_t end_xid,
                                         std::shared_ptr<XidIdRange> extent_node,
                                         bool insert)
    {
        // check eid max/min xid to see if they need to be fixed up.
        assert(extent_node->type == XidIdRange::IndexType::EXTENT);

        /* Need to check to see if a fixup of the existing extent_node is needed. */
        if (_check_start_range(extent_node, start_xid, end_xid, insert) ||
            _check_end_range(extent_node, start_xid, end_xid, insert)) {
            std::cout << "EID fixup\n";
            auto tid_itr = _tid_map.find(tid);
            assert(tid_itr != _tid_map.end());

            std::shared_ptr<XidIdRange> table_node = tid_itr->second;
            _fixup_set(eid, start_xid, end_xid, extent_node, table_node->children, insert);

            // check if we need to also fixup TID map
            if (_check_start_range(table_node, start_xid, end_xid, insert) ||
                _check_end_range(table_node, start_xid, end_xid, insert)) {
                std::cout << "TID fixup\n";
                _fixup_set(tid, start_xid, end_xid, table_node, _table_root, insert);
            }
        }
    }

    void
    WriteCacheTableSet::_fixup_set(uint64_t id, uint64_t start_xid, uint64_t end_xid,
                                   std::shared_ptr<XidIdRange> child_node,
                                   std::shared_ptr<XidIdRangeSet_t> set,
                                   bool insert)
    {
        // find lowerbound of set; first element in range [first, last)
        auto r = set->lower_bound(child_node);
        assert (r != set->end());

        std::cout << "searching for item to fixup: ID=" << id << ", XIDs=" << start_xid
                  << ":" << end_xid << std::endl;

        while (r != set->end()) {
            std::cout << ".";

            if ((*r)->id == child_node->id) {
                std::cout << "found item: XID range: " << child_node->start_xid << ":"
                          << child_node->end_xid << std::endl;

                // fixup element; set new start/end xid within xid_range node
                bool fixup = false;
                if (_check_start_range(child_node, start_xid, end_xid, insert)) {
                    if (insert) {
                        child_node->start_xid = start_xid;
                    } else {
                        child_node->start_xid = end_xid;
                    }
                    fixup = true;
                }

                if (_check_end_range(child_node, start_xid, end_xid, insert)) {
                    if (insert) {
                        child_node->end_xid = end_xid;
                    } else {
                        child_node->end_xid = start_xid;
                    }
                    fixup = true;
                }

                // remove from set and then add back
                if (fixup) {
                    set->erase(*r);
                    set->insert(child_node);
                }

                return;
            }
            r++;
        }
    }

    WriteCacheTableSet::rid_t
    WriteCacheTableSet::_get_rid_from_row(const std::string &pkey)
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
        _dump(_table_root);
        std::cout << std::endl;
    }

    void
    WriteCacheTableSet::_dump(std::shared_ptr<XidIdRangeSet_t> set)
    {
        for (auto x = set->begin(); x != set->end(); x++) {
            const auto &p = *x;
            p->dump();
            if (p->children) {
                _dump(p->children);
            }
        }
    }
}

static int s_id=0;

void
add_row(springtail::WriteCacheTableSet &ts, uint64_t tid, uint64_t eid, uint64_t xid, int rid=s_id)
{
    std::shared_ptr<springtail::WriteCacheIndexRow> row =
        std::make_shared<springtail::WriteCacheIndexRow>("data", fmt::format("key:{}", rid), xid, s_id);

    std::cout << fmt::format("\nInserting row: tid: {}, eid: {}, rid: {}, xid: {}, xid_seq: {}, key:{}\n",
                             tid, eid, rid, xid, s_id, rid);

    ts.add_row(tid, eid, row);

    if (rid == s_id) {
        s_id++;
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

    std::vector<std::shared_ptr<springtail::WriteCacheIndexRow>> rows = {};

    ts.get_rows(1, 1, 6, 9, 10, rows);

    add_row(ts, 1, 2, 8);

    add_row(ts, 1, 2, 7);

    add_row(ts, 1, 2, 15);

    ts.dump();

    ts.get_rows(1, 1, 6, 9, 10, rows);

    // update key:2 with xid 10
    add_row(ts, 1, 1, 10, 2);

    ts.dump();

    std::cout << "Evicting extent: tid=1, eid=2, xids: 8:15\n";
    ts.evict_extent(1, 2, 8, 15);

    ts.dump();

    std::cout << "Evicting extent: tid=1, eid=1, xids: 5:7\n";
    ts.evict_extent(1, 1, 5, 7);

    ts.dump();

    return 0;
}