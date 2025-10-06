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

    int
    WriteCacheTableSet::get_tids(uint64_t xid, uint32_t count,
                                 uint64_t start_offset, uint64_t &cursor,
                                 std::vector<uint64_t> &result)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG2, "Searching for TIDS in XID: {}", xid);

        int result_cnt = 0;
        cursor = 0;

        // lookup pg xid
        std::set<uint64_t> pg_xids = lookup_pgxid(xid);
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
        std::set<uint64_t> pg_xids = lookup_pgxid(xid, &md);

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

                result.push_back(std::make_shared<WriteCacheIndexExtent>(xid, (*itr)->id, (*itr)->data));
                itr++;
                count--;
            }
            read_lock.unlock();
        }

        return result_cnt;
    }
    
    std::vector<std::vector<WriteCacheIndexExtentPtr>>
    WriteCacheTableSet::get_all_extents(uint64_t tid, uint64_t xid, Metadata &md)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Searching for extents for XID: {}, TID: {}", xid, tid);

        int cursor = 0;
        int start_offset = 0;

        // lookup pg_xid
        std::set<uint64_t> pg_xids = lookup_pgxid(xid, &md);

        if (pg_xids.empty()) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "XID {} not found", xid);
            return {};
        }

        std::vector<std::vector<WriteCacheIndexExtentPtr>> result;

        // iterate through xids exclusive of start
        for (auto &pg_xid: pg_xids) {
            start_offset = cursor;
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Finding tids in PG XID: {}", pg_xid);

            // fetch xid node for this xid
            WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
            if (xid_node == nullptr) {
                LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "PG XID {} not found", pg_xid);
                return {};
            }

            WriteCacheIndexNodePtr tid_node = xid_node->find(tid);
            if (tid_node == nullptr) {
                LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "TID {} not found", tid);
                return {};
            }

            std::vector<WriteCacheIndexExtentPtr> extents;

            // iterate through children adding to result set
            std::shared_lock<std::shared_mutex> read_lock{tid_node->mutex};
            auto itr = tid_node->children.begin();
            while (itr != tid_node->children.end()) {
                cursor++;

                // check cursor offset, decr if above 0 and continue
                if (start_offset > 0) {
                    start_offset--;
                    itr++;
                    continue;
                }

                extents.push_back(std::make_shared<WriteCacheIndexExtent>(xid, (*itr)->id, (*itr)->data));
                itr++;
            }
            read_lock.unlock();
            result.push_back(extents);
        }

        return result;
    }
    void
    WriteCacheTableSet::evict_xid(uint64_t xid)
    {
        LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "Evicting XID: {}", xid);

        // lookup pg xid
        uint64_t pg_xid;
        std::unique_lock<std::shared_mutex> lock(_xid_map_mutex);
        auto itr = _xid_map.find(xid);
        if (itr == _xid_map.end()) {
            LOG_DEBUG(LOG_WRITE_CACHE_SERVER, LOG_LEVEL_DEBUG1, "XID {} not found", xid);
            return;
        }
        pg_xid = itr->second;
        _xid_map.erase(itr);
        _xid_ts_map.erase(xid);
        lock.unlock();

        abort(pg_xid);
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
        std::set<uint64_t> pg_xids = lookup_pgxid(xid);
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
    WriteCacheTableSet::drop_table(uint64_t tid, uint64_t pg_xid) {

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

    std::set<uint64_t>
    WriteCacheTableSet::lookup_pgxid(uint64_t pg_xid, Metadata *md)
    {
        // use a set to ensure xids are sorted
        std::set<uint64_t> result;
        std::shared_lock<std::shared_mutex> lock(_xid_map_mutex);
        auto range = _xid_map.equal_range(pg_xid);
        for (auto itr = range.first; itr != range.second; itr++) {
            result.insert(itr->second);
        }
        if (md != nullptr) {
            auto it = _xid_ts_map.find(pg_xid);
            if (it != _xid_ts_map.end()) {
                *md = it->second;
            }
        }
        return result;
    }
}
