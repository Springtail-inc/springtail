#include <cassert>

#include <common/logging.hh>
#include <write_cache/extent_mapper.hh>

namespace springtail {
    void
    TableExtentMapper::add_mapping(uint64_t target_xid,
                                   uint64_t old_eid,
                                   const std::vector<uint64_t> &new_eids)
    {
        std::vector<uint64_t> reverse_eids;

        // note: always lock forward before reverse to avoid deadlock
        boost::unique_lock lock(_mutex);

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
    TableExtentMapper::set_lookup(uint64_t target_xid,
                                  uint64_t extent_id)
    {
        boost::unique_lock lock(_mutex);

        // save the most recent XID that this extent ID was referenced at within the write cache
        auto it = _lookup_map.lower_bound(extent_id);
        if (it != _lookup_map.end() && it->first == extent_id) {
            _lookup_map[extent_id].latest_xid = target_xid;
        } else {
            _lookup_map.insert(it, { extent_id, { target_xid, target_xid } });
        }

        // store the XID mapping
        _xid_map[target_xid].push_back(extent_id);
    }

    std::vector<uint64_t>
    TableExtentMapper::forward_map(uint64_t target_xid,
                                   uint64_t extent_id)
    {
        boost::shared_lock lock(_mutex);

        // find the extent ID to see if there are changes to it prior to the target XID
        auto it = _forward_map.find(extent_id);
        if (it == _forward_map.end()) {
            // no changes at all, return empty vector
            return {};
        }

        // note: forward mapping is always done from the latest GC-2 checkpoint, so the XID of the
        //       mutation should always be before or at the target XID
        assert(it->second.back().xid <= target_xid);

        // store the set of new EIDs
        // note: updates are performed during add_mapping to ensure that this list is always
        //       up-to-date to the latest GC-2 changes
        return it->second.back().eids;
    }

    std::vector<uint64_t>
    TableExtentMapper::reverse_map(uint64_t access_xid,
                                   uint64_t target_xid,
                                   uint64_t extent_id) 
    {
        boost::shared_lock lock(_mutex);

        std::vector<uint64_t> cache_eids;

        // find earlier versions of the extent that were used for lookup in this XID range
        auto it = _reverse_map.find(extent_id);
        if (it != _reverse_map.end()) {
            // note: updates are performed during add_mapping() and expire() to keep this set as a
            //       superset of all potential extent IDs
            for (auto eid : it->second) {
                auto lookup_i = _lookup_map.find(eid);
                if (lookup_i != _lookup_map.end()
                    && lookup_i->second.latest_xid > access_xid
                    && lookup_i->second.start_xid <= target_xid) {
                    cache_eids.push_back(eid);
                }
            }
        }

        // check if there are future versions of this extent that were used for lookups
        auto f_it = _forward_map.find(extent_id);
        if (f_it != _forward_map.end()) {
            // go through all of the known mappings and see if they could exist in the write cache
            for (auto &entry : f_it->second) {
                if (entry.xid < target_xid) {
                    for (auto &eid : entry.eids) {
                        auto l = _lookup_map.find(eid);
                        if (l != _lookup_map.end()
                            && l->second.latest_xid > access_xid
                            && l->second.start_xid <= target_xid) {
                            cache_eids.push_back(eid);
                        }
                    }
                }
            }
        }

        return cache_eids;
    }

    bool
    TableExtentMapper::expire(uint64_t commit_xid)
    {
        // note: always lock lookup before forward, before reverse to avoid deadlock
        boost::unique_lock lock(_mutex);

        SPDLOG_DEBUG("Expire {}", commit_xid);

        // remove any metadata prior to the provided commit XID
        auto it = _xid_map.begin();
        while (it != _xid_map.end()) {
            SPDLOG_DEBUG("Check xid@{}", it->first);

            // stop once we go past the commit XID
            if (commit_xid < it->first) {
                break;
            }
            auto xid = it->first;

            for (auto eid : it->second) {
                SPDLOG_DEBUG("Cleanup {}", eid);

                // cleanup the lookup map
                auto l = _lookup_map.find(eid);
                assert(l != _lookup_map.end());

                // check if this extent ID has more references past this XID
                if (l->second.latest_xid <= xid) {
                    SPDLOG_DEBUG("Clear from lookup {}", l->first);

                    // clear the lookup map
                    _lookup_map.erase(l);

                    // get the set of new extent IDs from the forward map
                    auto f = _forward_map.find(eid);
                    assert(f != _forward_map.end());

                    for (auto &entry : f->second) {
                        SPDLOG_DEBUG("Try clear reverse xid@{}", entry.xid);

                        if (entry.xid > commit_xid) {
                            break; // stop once we are past the commit XID
                        }

                        // remove the extent ID from each of the reverse map entries of new extent IDs
                        for (auto new_eid : entry.eids) {
                            SPDLOG_DEBUG("Clear reverse entry {}", new_eid);

                            // find the reverse map entry
                            auto r = _reverse_map.find(new_eid);
                            assert(r != _reverse_map.end());
                            
                            // remove from the historical list
                            SPDLOG_DEBUG("Erase reverse entry {}", eid);
                            auto i = std::ranges::find(r->second, eid);
                            assert(i != r->second.end());
                            r->second.erase(i);

                            // if there are no historical extent IDs now, clear the entry
                            if (r->second.empty()) {
                                SPDLOG_DEBUG("Clear empty reverse {}", r->first);
                                _reverse_map.erase(r);
                            }
                        }
                    }

                    SPDLOG_DEBUG("Clear from forward {}", f->first);

                    // clear from the forward map
                    _forward_map.erase(f);
                }
            }

            // clean up the XID map
            _xid_map.erase(it);
            it = _xid_map.begin();
        }

        return _xid_map.empty();
    }

    /* static member initialization must happen outside of class */
    ExtentMapper* ExtentMapper::_instance = {nullptr};
    boost::mutex ExtentMapper::_instance_mutex;

    ExtentMapper *
    ExtentMapper::get_instance()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new ExtentMapper();
        }

        return _instance;
    }

    void
    ExtentMapper::shutdown()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    void
    ExtentMapper::add_mapping(uint64_t tid,
                              uint64_t target_xid,
                              uint64_t old_eid,
                              const std::vector<uint64_t> &new_eids)
    {
        Mapper mapper(this, tid, true);
        mapper->add_mapping(target_xid, old_eid, new_eids);
    }

    void
    ExtentMapper::set_lookup(uint64_t tid,
                             uint64_t target_xid,
                             uint64_t extent_id)
    {
        Mapper mapper(this, tid, true);
        mapper->set_lookup(target_xid, extent_id);
    }

    std::vector<uint64_t>
    ExtentMapper::forward_map(uint64_t tid,
                              uint64_t target_xid,
                              uint64_t extent_id)
    {
        // note: no need to call _put_table() when not is_write
        Mapper mapper(this, tid);
        if (mapper.is_null()) {
            return {};
        }

        return mapper->forward_map(target_xid, extent_id);
    }

    std::vector<uint64_t>
    ExtentMapper::reverse_map(uint64_t tid,
                              uint64_t access_xid,
                              uint64_t target_xid,
                              uint64_t extent_id)
    {
        // note: no need to call _put_table() when not is_write
        Mapper mapper(this, tid);
        if (mapper.is_null()) {
            return {};
        }

        return mapper->reverse_map(access_xid, target_xid, extent_id);
    }

    void
    ExtentMapper::expire(uint64_t tid,
                         uint64_t commit_xid)
    {
        Mapper mapper(this, tid, true);
        bool is_empty = mapper->expire(commit_xid);

        // if the mapping is empty, attempt to evict
        if (is_empty) {
            mapper.try_evict();
        }
    }

    std::shared_ptr<TableExtentMapper>
    ExtentMapper::_get_table(uint64_t tid, bool is_write)
    {
        boost::unique_lock lock(_mutex);

        auto table_i = _table_map.find(tid);
        if (table_i == _table_map.end()) {
            if (!is_write) {
                return nullptr;
            }

            auto result = _table_map.insert({ tid, TableEntry() });
            assert(result.second);

            table_i = result.first;
            table_i->second.second = std::make_shared<TableExtentMapper>();
        }

        if (is_write) {
            ++table_i->second.first;
        }

        return table_i->second.second;
    }

    void
    ExtentMapper::_put_table(uint64_t tid, bool is_expire) noexcept
    {
        boost::unique_lock lock(_mutex);

        auto table_i = _table_map.find(tid);
        assert(table_i != _table_map.end());

        // note: should only be called when is_write = true on _get_table()
        --table_i->second.first;

        if (is_expire && table_i->second.first == 0) {
            _table_map.erase(table_i);
        }
    }
}
