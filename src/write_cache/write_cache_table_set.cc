#include <ranges>

#include <common/common.hh>
#include <common/logging.hh>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail
{

    WriteCacheTableSet::WriteCacheTableSet(const std::filesystem::path &db_dir_path, int row_table_partitions) :
        _db_dir_path(db_dir_path),
        _xid_root(std::make_shared<WriteCacheIndexNode>(-1, WriteCacheIndexNode::IndexType::ROOT))
    {}

    void
    WriteCacheTableSet::add_extent(uint64_t tid,
                                   uint64_t pg_xid,
                                   uint64_t lsn,
                                   const ExtentPtr data)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Inserting: extent for TID: {}, PG XID: {}, LSN: {}", tid, pg_xid, lsn);

        // find the xid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr xid_node = _xid_root->findAdd(pg_xid, WriteCacheIndexNode::IndexType::XID);

        // find the tid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr tid_node = xid_node->findAdd(tid, WriteCacheIndexNode::IndexType::TABLE);

        // add data to the tid node
        tid_node->add(std::make_shared<WriteCacheIndexNode>(lsn, data));
    }

    void
    WriteCacheTableSet::add_extent_on_disk(uint64_t tid, uint64_t pg_xid, uint64_t lsn, uint64_t extent_offset, size_t extent_size)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Inserting: extent on disk for TID: {}, PG XID: {}, LSN: {}", tid, pg_xid, lsn);

        // find the xid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr xid_node = _xid_root->findAdd(pg_xid, WriteCacheIndexNode::IndexType::XID);

        // find the tid node, if not exists, create a node with given ID and return it
        WriteCacheIndexNodePtr tid_node = xid_node->findAdd(tid, WriteCacheIndexNode::IndexType::TABLE);

        // add data to the tid node
        tid_node->add(std::make_shared<WriteCacheIndexNode>(lsn, extent_offset, extent_size));
    }

    int
    WriteCacheTableSet::get_tids(uint64_t xid, uint32_t count,
                                 uint64_t start_offset, uint64_t &cursor,
                                 std::vector<uint64_t> &result)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG2, "Searching for TIDS in XID: {}", xid);

        int result_cnt = 0;
        cursor = 0;

        // lookup pg xid
        std::set<uint64_t> pg_xids = _lookup_pgxid(xid);
        if (pg_xids.empty()) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "XID {} not found", xid);
            return 0;
        }

        // iterate through xids exclusive of start
        for (auto &pg_xid: pg_xids) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG2, "Finding tids in PG XID: {}", pg_xid);

            // fetch xid node for this xid and read lock it
            WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
            if (xid_node == nullptr) {
                LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG2, "PG XID {} not found", pg_xid);
                continue;
            }

            // iterate through children adding to result set
            std::shared_lock<std::shared_mutex> read_lock{xid_node->mutex};
            auto itr = xid_node->children.begin();
            while (itr != xid_node->children.end() && count > 0) {
                cursor++;

                // check cursor offset, decr if above 0 and continue
                if (start_offset > 0) {
                    start_offset--;
                    itr++;
                    continue;
                }

                result.push_back((*itr)->id);
                result_cnt++;
                itr++;
                count--;
            }
            read_lock.unlock();
        }

        return result_cnt;
    }

    int
    WriteCacheTableSet::get_extents(uint64_t tid, uint64_t xid, uint32_t count,
                                    uint64_t start_offset, uint64_t &cursor,
                                    std::vector<WriteCacheIndexExtentPtr> &result, Metadata &md)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Searching for extents for XID: {}, TID: {}", xid, tid);

        int result_cnt = 0;
        cursor = 0;

        // lookup pg_xid
        std::set<uint64_t> pg_xids = _lookup_pgxid(xid, &md);

        if (pg_xids.empty()) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "XID {} not found", xid);
            return 0;
        }

        // iterate through xids exclusive of start
        for (auto &pg_xid: pg_xids) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Finding tids in PG XID: {}", pg_xid);

            // fetch xid node for this xid
            WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
            if (xid_node == nullptr) {
                LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "PG XID {} not found", pg_xid);
                return 0;
            }

            WriteCacheIndexNodePtr tid_node = xid_node->find(tid);
            if (tid_node == nullptr) {
                LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "TID {} not found", tid);
                return 0;
            }

            // iterate through children adding to result set
            std::shared_lock<std::shared_mutex> read_lock{tid_node->mutex};
            auto itr = tid_node->children.begin();
            while (itr != tid_node->children.end() && count > 0) {
                cursor++;

                // check cursor offset, decr if above 0 and continue
                if (start_offset > 0) {
                    start_offset--;
                    itr++;
                    result_cnt++;
                    continue;
                }

                if ((*itr)->type == WriteCacheIndexNode::EXTENT) {
                    // get extent from memory
                    result.push_back(std::make_shared<WriteCacheIndexExtent>(xid, (*itr)->id, (*itr)->data));
                } else if ((*itr)->type == WriteCacheIndexNode::EXTENT_ON_DISK) {
                    // get extent from disk
                    std::filesystem::path file_name = _db_dir_path / std::to_string(pg_xid);
                    auto handle = IOMgr::get_instance()->open(file_name, IOMgr::READ, false);
                    auto response = handle->read((*itr)->data_offset);
                    CHECK(response->status == SUCCESS);
                    std::shared_ptr<Extent> extent = std::make_shared<Extent>(response->data);
                    result.push_back(std::make_shared<WriteCacheIndexExtent>(xid, (*itr)->id, extent));
                } else {
                    CHECK(false) << "Invalid extent type " << (*itr)->type_to_str();
                }
                itr++;
                count--;
            }
            read_lock.unlock();
        }

        return result_cnt;
    }

    void
    WriteCacheTableSet::evict_xid(uint64_t xid)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Evicting XID: {}", xid);

        // lookup pg xid
        std::unique_lock<std::shared_mutex> lock(_xid_map_mutex);
        auto [begin, end] = _xid_map.equal_range(xid);
        if (begin == _xid_map.end()){
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "XID {} not found", xid);
            return;
        }

        // populate pg_xids vector
        std::vector<uint64_t> pg_xids;
        pg_xids.reserve(std::distance(begin, end));
        std::ranges::transform(
            std::ranges::subrange(begin, end),
            std::back_inserter(pg_xids),
            &std::remove_reference_t<decltype(*begin)>::second);

        // cleanup maps
        _xid_map.erase(begin, end);
        _xid_ts_map.erase(xid);
        lock.unlock();

        // abort all pg_xids
        std::ranges::for_each(pg_xids, [this](uint64_t pg_xid) { abort(pg_xid); });
    }

    void
    WriteCacheTableSet::abort(uint64_t pg_xid)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Aborting PG XID: {}", pg_xid);

        // fetch xid node for this xid if exists
        WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
        if (xid_node == nullptr) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "XID {} not found", pg_xid);
            return;
        }

        // remove xid node
        _xid_root->remove(xid_node);
    }

    void
    WriteCacheTableSet::commit(uint64_t pg_xid, uint64_t xid, Metadata md)
    {
        LOG_INFO("Committing PG XID: {} -> XID: {}", pg_xid, xid);

        // insert xid into xid map
        // XXX should we check if there is an existing pg_xid with data and skip if not?
        std::unique_lock<std::shared_mutex> lock(_xid_map_mutex);
        _xid_map.insert({xid, pg_xid});
        _xid_ts_map.insert({xid, std::move(md)});
    }

    void
    WriteCacheTableSet::commit(const std::vector<uint64_t>& pg_xids, uint64_t xid, Metadata md)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Committing multiple PG XIDs -> XID: {}", xid);
        // insert xid into xid map
        // XXX should we check if there is an existing pg_xid with data and skip if not?
        std::unique_lock<std::shared_mutex> lock(_xid_map_mutex);
        for (auto &pg_xid: pg_xids) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG3, "Committing PG XID: {} -> XID: {}", pg_xid, xid);
            _xid_map.insert({xid, pg_xid});
        }
        _xid_ts_map.insert({xid, std::move(md)});
    }

    void
    WriteCacheTableSet::evict_table(uint64_t tid, uint64_t xid)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Evicting table: TID={} XID={}", tid, xid);

        // lookup xid in xid map
        std::set<uint64_t> pg_xids = _lookup_pgxid(xid);
        if (pg_xids.empty()) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "XID {} not found", xid);
            return;
        }

        for (auto &pg_xid: pg_xids) {
            drop_table(tid, pg_xid);
        }

        // XXX should we remove xid from xid map if no more tids?
    }

    void
    WriteCacheTableSet::drop_table(uint64_t tid, uint64_t pg_xid)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Dropping table: TID={} PG_XID={}", tid, pg_xid);

        // fetch xid node for this xid if exists
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Searching for PG XID: {}", pg_xid);
        WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
        if (xid_node == nullptr) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "PG XID {} not found", pg_xid);
            return;
        }

        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Removing TID: {}", tid);
        xid_node->remove(tid);

        if (xid_node->children.size() == 0) {
            _xid_root->remove_child_if_empty(xid_node);
        }
    }

    void
    WriteCacheTableSet::dump()
    {
        std::cout << "\nDumping table\n";
        _dump(_xid_root);
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

    uint64_t
    WriteCacheTableSet::get_memory_size(uint64_t pg_xid)
    {
        uint64_t memory_size = 0;
        // find the xid node
        WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
        if (xid_node == nullptr) {
            return memory_size;
        }
        // iterate over table nodes
        for (auto &table_node: xid_node->children) {
            DCHECK(table_node->type == WriteCacheIndexNode::TABLE);

            // iterate over extent nodes
            for (auto &extent_node: table_node->children) {

                // only add the size of the extents stored in memory
                if (extent_node->type == WriteCacheIndexNode::EXTENT) {
                    memory_size += extent_node->data->byte_count();
                }
            }
        }
        return memory_size;
    }

    size_t
    WriteCacheTableSet::get_memory_size(uint64_t tid, uint64_t xid)
    {
        uint64_t memory_size = 0;
        // lookup xid in xid map
        std::set<uint64_t> pg_xids = _lookup_pgxid(xid);
        if (pg_xids.empty()) {
            return memory_size;
        }
        for (auto &pg_xid: pg_xids) {
            memory_size += get_memory_size_for_pg_xid(tid, pg_xid);
        }
        return memory_size;
    }

    size_t
    WriteCacheTableSet::get_memory_size_for_pg_xid(uint64_t tid, uint64_t pg_xid)
    {
        uint64_t memory_size = 0;
        WriteCacheIndexNodePtr pg_xid_node = _xid_root->find(pg_xid);
        if (pg_xid_node == nullptr) {
            return memory_size;
        }
        WriteCacheIndexNodePtr tid_node = pg_xid_node->find(tid);
        if (tid_node == nullptr) {
            return memory_size;
        }
        for (auto &extent_node: tid_node->children) {
            if (extent_node->type == WriteCacheIndexNode::EXTENT) {
                memory_size += extent_node->data->byte_count();
            }
        }
        return memory_size;
    }

    nlohmann::json
    WriteCacheTableSet::get_stats()
    {
        nlohmann::json stats = nlohmann::json::object();
        nlohmann::json xid_stats = nlohmann::json::object();
        for (const auto& [key, value] : _xid_map) {
            // Ensure an array exists for this key
            if (!xid_stats.contains(std::to_string(key))) {
                xid_stats[std::to_string(key)] = nlohmann::json::array();
            }

            // Append the value
            xid_stats[std::to_string(key)].push_back(value);
        }
        stats["xid map"] = xid_stats;

        nlohmann::json cache_stats = nlohmann::json::object();
        for (auto &pg_xid_node: _xid_root->children) {
            std::string pg_xid_name = pg_xid_node->type_to_str() + ":" + std::to_string(pg_xid_node->id);
            nlohmann::json pg_xid_node_stats = nlohmann::json::object();
            for (auto &tid_node: pg_xid_node->children) {
                std::string tid_name = tid_node->type_to_str() + ":" + std::to_string(tid_node->id);
                nlohmann::json tid_node_stats = nlohmann::json::object();
                for (auto &extent_node: tid_node->children) {
                    std::string extent_name = extent_node->type_to_str() + ":" + std::to_string(extent_node->id);
                    nlohmann::json extent_node_stats = nlohmann::json::object();
                    if (extent_node->type == WriteCacheIndexNode::EXTENT) {
                        extent_node_stats["extent size"] = extent_node->data->byte_count();
                    } else if (extent_node->type == WriteCacheIndexNode::EXTENT_ON_DISK) {
                        extent_node_stats["data offset"] = extent_node->data_offset;
                        extent_node_stats["data size"] = extent_node->data_size;
                    }
                    tid_node_stats[extent_name] = extent_node_stats;
                }
                pg_xid_node_stats[tid_name] = tid_node_stats;
            }
            cache_stats[pg_xid_name] = pg_xid_node_stats;
        }
        stats["cache"] = cache_stats;
        return stats;
    }

    std::set<uint64_t>
    WriteCacheTableSet::_lookup_pgxid(uint64_t xid, Metadata *md)
    {
        // use a set to ensure xids are sorted
        std::set<uint64_t> result;
        std::shared_lock<std::shared_mutex> lock(_xid_map_mutex);
        auto range = _xid_map.equal_range(xid);
        for (auto itr = range.first; itr != range.second; itr++) {
            result.insert(itr->second);
        }
        if (md != nullptr) {
            auto it = _xid_ts_map.find(xid);
            if (it != _xid_ts_map.end()) {
                *md = it->second;
            }
        }
        return result;
    }
}
