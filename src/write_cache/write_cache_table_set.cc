#include <vector>
#include <memory>
#include <mutex>
#include <cassert>
#include <iostream>

#include <xxhash.h>
#include <fmt/core.h>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail
{
    void
    WriteCacheTableSet::add_row(uint64_t tid, uint64_t eid, std::shared_ptr<WriteCacheIndexRow> data)
    {
        // get the rid for the row
        rid_t rid = _get_rid_from_row(data->pkey);

        uint64_t xid = data->xid;

        // add row into row data -- note: an emplace may be done on data, so not safe to access after this.
        if (_insert_row_data(tid, rid, data)) {
            // data already existed at the xid, so no need to add to additional maps
            return;
        }

        // need to add to EID map
        // lookup xid range struct in eid map and update
        _insert_eid_map(tid, eid, rid, xid);

        // if not there, lookup xid range in tid map
        // if not there, insert into table_root, and tid map; insert into eid map
    }

    bool
    WriteCacheTableSet::_insert_row_data(uint64_t tid, rid_t rid,
                                         std::shared_ptr<WriteCacheIndexRow> data)
    {
        // generate key
        std::string key = fmt::format("{}:{}:{}", tid, rid, data->xid);
        std::unique_lock lock{_row_data_mutex};

        auto p = _row_data_map.insert_or_assign(key, data);
        if (!p.second) {
            // an entry already exists, so new data was not inserted
            // p.first holds node (pair<key,value>); check seq of existing element
            if ((*p.first).second->xid_seq < data->xid_seq) {
                _row_data_map.emplace_hint(p.first, key, data);
            }
            return true;
        }

        return false; // no data existed
    }

    void
    WriteCacheTableSet::_insert_tid_map(uint64_t tid, uint64_t xid,
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
            _fixup_set(tid, xid, tid_itr->second, _table_root);
        }
    }

    void
    WriteCacheTableSet::_insert_eid_map(uint64_t tid, uint64_t eid, rid_t rid, uint64_t xid)
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
            _insert_tid_map(tid, xid, eid_xid_range);

            // add eid range to eid_map
            _eid_map.emplace(key, eid_xid_range);
        } else {
            // extent found in eid map
            // create row entry and add it to eid children
            std::shared_ptr<XidIdRange> rid_xid_range =
                std::make_shared<XidIdRange>(xid, xid, rid, eid);

            // add row range to eid children
            eid_itr->second->children->insert(rid_xid_range);

            // check eid max/min xid to see if they need to be fixed up.
            if (eid_itr->second->start_xid > xid || eid_itr->second->end_xid < xid) {
                auto tid_itr = _tid_map.find(tid);
                _fixup_set(eid, xid, eid_itr->second, tid_itr->second->children);
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

        while (r != set->end()) {
            if ((*r)->id == xid_range->id) {
                // fixup element
                if (xid_range->start_xid > xid) {
                    xid_range->start_xid = xid;
                }
                if (xid_range->end_xid < xid) {
                    xid_range->end_xid = xid;
                }

                // remove from set and then add back
                set->erase(*r);
                set->insert(xid_range);
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
    WriteCacheTableSet::dump(std::shared_ptr<std::set<std::shared_ptr<XidIdRange>, XidIdRange::XidIdRangeComparator>> set)
    {
        for (auto x = set->begin(); x != set->end(); x++) {
            auto p = *x;
            std::cout << "Level: " << p->type << " ID: " << std::get<uint64_t>(p->id) << " P_ID: " << p->p_id << " XIDs: " << p->start_xid << ":" << p->end_xid << std::endl;
            if (p->children) {
                dump(p->children);
            }
        }
    }

}