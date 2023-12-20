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
        _table_root(std::make_shared<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>>())
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

    void
    WriteCacheTableSet::get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid)
    {
        std::string eid_key = fmt::format("{}:{}", tid, eid);

        auto eid_itr = _eid_map.find(eid_key);
        if (eid_itr == _eid_map.end()) {
            // nothing here
            return;
        }

        assert(eid_itr->second->type == IndexType::EXTENT);
        assert(std::get<uint64_t>(eid_itr->second->id) == eid);

        std::cout << "Searching for rows in range: " << start_xid << ":" << end_xid << std::endl;

        std::shared_ptr<XidIdRange> search_range = std::make_shared<XidIdRange>(start_xid, end_xid);
        auto search_itr = eid_itr->second->children->lower_bound(search_range);
        while (search_itr != eid_itr->second->children->end()) {
            assert((*search_itr)->p_id == eid);
            std::cout << "Row ID: " << std::get<rid_t>((*search_itr)->id) << ", Xid range: " << (*search_itr)->start_xid << ":" << (*search_itr)->end_xid << std::endl;
            search_itr++;
        }
    }

    bool
    WriteCacheTableSet::_insert_row_data(uint64_t tid, uint64_t eid, rid_t rid,
                                         std::shared_ptr<WriteCacheIndexRow> data)
    {
        // generate key
        std::string key = fmt::format("{}:{}:{}", tid, rid, data->xid);
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
                std::make_shared<XidIdRange>(xid, xid, IndexType::TABLE, tid);

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
                _fixup_set(tid, xid, tid_itr->second, _table_root);
            }
        }
    }

    void
    WriteCacheTableSet::_insert_extent_index(uint64_t tid, uint64_t eid,
                                             rid_t rid, uint64_t xid)
    {
        std::string key = fmt::format("{}:{}", tid, eid);
        auto eid_itr = _eid_map.find(key);
        if (eid_itr == _eid_map.end()) {
            // entry not found, create entry
            // create eid range
            std::shared_ptr<XidIdRange> eid_xid_range =
                std::make_shared<XidIdRange>(xid, xid, IndexType::EXTENT, eid, tid);

            // create row entry
            std::shared_ptr<XidIdRange> rid_xid_range =
                std::make_shared<XidIdRange>(xid, xid, rid, eid);

            // add row entry to extent children set
            eid_xid_range->children->insert(rid_xid_range);

            // add new eid_xid range to the tid map
            _insert_table_index(tid, xid, eid_xid_range);

            // add eid range to eid_map
            _eid_map.emplace(key, eid_xid_range);
        } else {
            std::cout << "Found eid, adding row\n";
            // extent found in eid map
            // create row entry and add it to eid children
            std::shared_ptr<XidIdRange> rid_xid_range =
                std::make_shared<XidIdRange>(xid, xid, rid, eid);

            // add row range to eid children
            eid_itr->second->children->insert(rid_xid_range);

            // check eid and tid ranges to see if they need to be fixed up based on new xid
            _fixup_eid_range(tid, eid, xid, eid_itr->second);
        }
    }

    void
    WriteCacheTableSet::_fixup_eid_range(uint64_t tid, uint64_t eid, uint64_t xid,
                                         std::shared_ptr<XidIdRange> xid_range)
    {
        // check eid max/min xid to see if they need to be fixed up.
        assert(xid_range->type == IndexType::EXTENT);

        if (xid_range->start_xid > xid || xid_range->end_xid < xid) {
            std::cout << "EID fixup\n";
            auto tid_itr = _tid_map.find(tid);
            assert(tid_itr != _tid_map.end());
            _fixup_set(eid, xid, xid_range, tid_itr->second->children);

            // check if we need to also fixup TID map
            if (tid_itr->second->start_xid > xid || tid_itr->second->end_xid < xid) {
                std::cout << "TID fixup\n";
                _fixup_set(tid, xid, tid_itr->second, _table_root);
            }
        }
    }

    void
    WriteCacheTableSet::_fixup_set(uint64_t id, uint64_t xid,
                                   std::shared_ptr<XidIdRange> xid_range,
                                   std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>> set)
    {
        auto r = set->lower_bound(xid_range);
        assert (r != set->end());

        std::cout << "searching for item to fixup: ID=" << id << ", XID=" << xid << std::endl;

        while (r != set->end()) {
            std::cout << ".";

            if ((*r)->id == xid_range->id) {
                std::cout << "found item: XID range: " << xid_range->start_xid << ":" << xid_range->end_xid << std::endl;

                // fixup element
                bool fixup = false;
                if (xid_range->start_xid > xid) {
                    xid_range->start_xid = xid;
                    fixup = true;
                }
                if (xid_range->end_xid < xid) {
                    xid_range->end_xid = xid;
                    fixup = true;
                }

                // remove from set and then add back
                if (fixup) {
                    set->erase(*r);
                    set->insert(xid_range);
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
    WriteCacheTableSet::_dump(std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>> set)
    {
        for (auto x = set->begin(); x != set->end(); x++) {
            const auto &p = *x;
            std::string type;
            std::string id;
            if (p->type == IndexType::TABLE) {
                type = "table";
                id = fmt::format("{}", std::get<uint64_t>(p->id));
            } else if (p->type == IndexType::EXTENT) {
                type = "extent";
                id = fmt::format("{}", std::get<uint64_t>(p->id));
            } else if (p->type == IndexType::ROW) {
                type = "row";
                id = std::get<rid_t>(p->id);
            }

            std::cout << "Level: " << type << " ID: " << id << " P_ID: " << p->p_id << " XIDs: " << p->start_xid << ":" << p->end_xid << std::endl;
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

    add_row(ts, 1, 1, 5);
    add_row(ts, 1, 1, 6);
    add_row(ts, 1, 1, 9);
    add_row(ts, 1, 1, 7);
    add_row(ts, 1, 1, 10);

    ts.dump();

    ts.get_rows(1, 1, 6, 9);

    add_row(ts, 1, 2, 8);

    add_row(ts, 1, 2, 7);

    add_row(ts, 1, 2, 15);

    ts.dump();

    ts.get_rows(1, 1, 6, 9);

    // update key:2 with xid 10
    add_row(ts, 1, 1, 10, 2);

    ts.dump();

    return 0;
}