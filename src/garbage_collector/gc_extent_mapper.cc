#include <cassert>

#include <garbage_collector/gc_extent_mapper.hh>

namespace springtail::gc {
    void
    ExtentMapper::add_mapping(uint64_t target_xid,
                              uint64_t old_eid,
                              const std::vector<uint64_t> &new_eids)
    {
        std::vector<uint64_t> reverse_eids;

        // record the mapping from old_eid -> new_eids at target_xid
        _forward_map[old_eid].push_back({ target_xid, new_eids });

        // also update any earlier versions of the old_eid
        auto it = _reverse_map.find(old_eid);
        if (it != _reverse_map.end()) {
            for (auto eid : it->second) {
                _forward_map[eid].push_back({ target_xid, new_eids });
            }

            // save the reverse mapping of the old extent ID
            reverse_eids = it->second;
        }

        // construct the reverse mapping
        reverse_eids.push_back(old_eid);
        for (auto eid : new_eids) {
            _reverse_map[eid] = reverse_eids;
        }
    }

    void
    ExtentMapper::set_lookup(uint64_t target_xid,
                             uint64_t extent_id)
    {
        // save the most recent XID that this extent ID was referenced at within the write cache
        _lookup_map[extent_id] = target_xid;

        // store the XID mapping
        _xid_map[target_xid].push_back(extent_id);
    }

    std::vector<uint64_t>
    ExtentMapper::forward_map(uint64_t target_xid,
                              uint64_t old_eid)
    {
        // find the extent ID to see if there are changes to it prior to the target XID
        auto it = _forward_map.find(old_eid);
        if (it == _forward_map.end()) {
            // no changes at all, return empty vector
            return {};
        }

        // note: forward mapping is always done from the latest GC-2 checkpoint, so the XID of the
        //       mutation should always be before the target XID
        assert(it->second.back().xid < target_xid);

        // store the set of new EIDs
        // note: updates are performed during add_mapping to ensure that this list is always
        //       up-to-date to the latest GC-2 changes
        return it->second.back().eids;
    }

    std::vector<uint64_t>
    ExtentMapper::reverse_map(uint64_t access_xid,
                              uint64_t target_xid,
                              uint64_t extent_id)
    {
        // find earlier versions of this extent
        auto it = _reverse_map.find(extent_id);
        if (it == _reverse_map.end()) {
            return {}; // no known earlier version of the extent ID
        }

        // return the set of known historic extent IDs
        // note: updates are performed during add_mapping() and expire() to keep this set accurate and minimized
        return it->second;
    }

    void
    ExtentMapper::expire(uint64_t commit_xid)
    {
        // remove any metadata prior to the provided commit XID
        auto it = _xid_map.begin();
        while (it != _xid_map.end()) {
            // stop once we go past the commit XID
            if (commit_xid < it->first) {
                break;
            }
            auto xid = it->first;

            for (auto eid : it->second) {
                // cleanup the lookup map
                auto l = _lookup_map.find(eid);
                assert(l != _lookup_map.end());

                // check if this extent ID has more references past this XID
                if (l->second <= xid) {
                    // clear the lookup map
                    _lookup_map.erase(l);

                    // get the set of new extent IDs from the forward map
                    auto f = _forward_map.find(eid);
                    assert(f != _forward_map.end());

                    for (auto &entry : f->second) {
                        if (entry.xid > commit_xid) {
                            break; // stop once we are past the commit XID
                        }

                        // remove the extent ID from each of the reverse map entries of new extent IDs
                        for (auto new_eid : entry.eids) {
                            // find the reverse map entry
                            auto r = _reverse_map.find(new_eid);
                            assert(r != _reverse_map.end());
                            
                            // remove from the historical list
                            auto i = std::ranges::find(r->second, eid);
                            assert(i != r->second.end());
                            r->second.erase(i);

                            // if there are no historical extent IDs now, clear the entry
                            if (r->second.empty()) {
                                _reverse_map.erase(r);
                            }
                        }
                    }

                    // clear from the forward map
                    _forward_map.erase(f);
                }
            }

            // clean up the XID map
            _xid_map.erase(it);
            it = _xid_map.begin();
        }
    }
}
