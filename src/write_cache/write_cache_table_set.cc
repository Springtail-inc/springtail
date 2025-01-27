#include <fmt/core.h>

#include <cassert>
#include <common/common.hh>
#include <common/logging.hh>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail {

WriteCacheTableSet::WriteCacheTableSet(int row_table_partitions)
    : _xid_root(std::make_shared<WriteCacheIndexNode>(-1, WriteCacheIndexNode::IndexType::ROOT))
{
}

void
WriteCacheTableSet::add_extent(uint64_t tid, uint64_t pg_xid, uint64_t lsn, const ExtentPtr data)
{
    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER,
                        "Inserting: extent for TID: {}, PG XID: {}, LSN: {}\n", tid, pg_xid, lsn);

    // find the xid node, if not exists, create a node with given ID and return it
    WriteCacheIndexNodePtr xid_node =
        _xid_root->findAdd(pg_xid, WriteCacheIndexNode::IndexType::XID);

    // find the tid node, if not exists, create a node with given ID and return it
    WriteCacheIndexNodePtr tid_node = xid_node->findAdd(tid, WriteCacheIndexNode::IndexType::TABLE);

    // add data to the tid node
    tid_node->add(std::make_shared<WriteCacheIndexNode>(lsn, data));
}

int
WriteCacheTableSet::get_tids(uint64_t xid,
                             uint32_t count,
                             uint64_t start_offset,
                             uint64_t &cursor,
                             std::vector<uint64_t> &result)
{
    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Searching for TIDS in XID: {}\n", xid);

    int result_cnt = 0;
    cursor = 0;

    // lookup pg xid
    std::set<uint64_t> pg_xids = lookup_pgxid(xid);
    if (pg_xids.empty()) {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "XID {} not found\n", xid);
        return 0;
    }

    // iterate through xids exclusive of start
    for (auto &pg_xid : pg_xids) {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Finding tids in PG XID: {}\n", pg_xid);

        // fetch xid node for this xid and read lock it
        WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
        if (xid_node == nullptr) {
            SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "PG XID {} not found\n", pg_xid);
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
WriteCacheTableSet::get_extents(uint64_t tid,
                                uint64_t xid,
                                uint32_t count,
                                uint64_t start_offset,
                                uint64_t &cursor,
                                std::vector<WriteCacheIndexExtentPtr> &result)
{
    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Searching for extents for XID: {}, TID: {}\n", xid,
                        tid);

    int result_cnt = 0;
    cursor = 0;

    // lookup pg_xid
    std::set<uint64_t> pg_xids = lookup_pgxid(xid);
    if (pg_xids.empty()) {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "XID {} not found\n", xid);
        return 0;
    }

    // iterate through xids exclusive of start
    for (auto &pg_xid : pg_xids) {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Finding tids in PG XID: {}\n", pg_xid);

        // fetch xid node for this xid
        WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
        if (xid_node == nullptr) {
            SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "PG XID {} not found\n", pg_xid);
            return 0;
        }

        WriteCacheIndexNodePtr tid_node = xid_node->find(tid);
        if (tid_node == nullptr) {
            SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "TID {} not found\n", tid);
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

            result.push_back(
                std::make_shared<WriteCacheIndexExtent>(xid, (*itr)->id, (*itr)->data));
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
    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Evicting XID: {}\n", xid);

    // lookup pg xid
    uint64_t pg_xid;
    std::unique_lock<std::shared_mutex> lock(_xid_map_mutex);
    auto itr = _xid_map.find(xid);
    if (itr == _xid_map.end()) {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "XID {} not found\n", xid);
        return;
    }
    pg_xid = itr->second;
    _xid_map.erase(itr);
    lock.unlock();

    abort(pg_xid);
}

void
WriteCacheTableSet::abort(uint64_t pg_xid)
{
    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Aborting PG XID: {}\n", pg_xid);

    // fetch xid node for this xid if exists
    WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
    if (xid_node == nullptr) {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "XID {} not found\n", pg_xid);
        return;
    }

    // remove xid node
    _xid_root->remove(xid_node);
}

void
WriteCacheTableSet::commit(uint64_t pg_xid, uint64_t xid)
{
    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Committing PG XID: {} -> XID: {}\n", pg_xid, xid);

    // insert xid into xid map
    // XXX should we check if there is an existing pg_xid with data and skip if not?
    std::unique_lock<std::shared_mutex> lock(_xid_map_mutex);
    _xid_map.insert({xid, pg_xid});
}

void
WriteCacheTableSet::commit(std::vector<uint64_t> pg_xids, uint64_t xid)
{
    // insert xid into xid map
    // XXX should we check if there is an existing pg_xid with data and skip if not?
    std::unique_lock<std::shared_mutex> lock(_xid_map_mutex);
    for (auto &pg_xid : pg_xids) {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Committing PG XID: {} -> XID: {}\n", pg_xid,
                            xid);
        _xid_map.insert({xid, pg_xid});
    }
}

void
WriteCacheTableSet::evict_table(uint64_t tid, uint64_t xid)
{
    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Evicting table: TID={} XID={}", tid, xid);

    // lookup xid in xid map
    std::set<uint64_t> pg_xids = lookup_pgxid(xid);
    if (pg_xids.empty()) {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "XID {} not found\n", xid);
        return;
    }

    for (auto &pg_xid : pg_xids) {
        drop_table(tid, pg_xid);
    }

    // XXX should we remove xid from xid map if no more tids?
}

void
WriteCacheTableSet::drop_table(uint64_t tid, uint64_t pg_xid)
{
    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Dropping table: TID={} PG_XID={}", tid, pg_xid);

    // fetch xid node for this xid if exists
    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Searching for PG XID: {}\n", pg_xid);
    WriteCacheIndexNodePtr xid_node = _xid_root->find(pg_xid);
    if (xid_node == nullptr) {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "PG XID {} not found\n", pg_xid);
        return;
    }

    SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Removing TID: {}\n", tid);
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
WriteCacheTableSet::lookup_pgxid(uint64_t pg_xid)
{
    // use a set to ensure xids are sorted
    std::set<uint64_t> result;
    std::shared_lock<std::shared_mutex> lock(_xid_map_mutex);
    auto range = _xid_map.equal_range(pg_xid);
    for (auto itr = range.first; itr != range.second; itr++) {
        result.insert(itr->second);
    }
    return result;
}
}  // namespace springtail
