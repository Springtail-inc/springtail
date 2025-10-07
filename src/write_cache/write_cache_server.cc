#include <boost/thread/locks.hpp>

#include <common/json.hh>
#include <common/properties.hh>
#include <write_cache/write_cache_server.hh>
#include <write_cache/write_cache_service.hh>

namespace springtail {

WriteCacheServer::WriteCacheServer() : Singleton<WriteCacheServer>(ServiceId::WriteCacheServerId)
{
    auto json = Properties::get(Properties::WRITE_CACHE_CONFIG);

    // find storage directory path name
    Json::get_to(json, "disk_storage_dir", _disk_storage_dir);
    Json::get_to(json, "memory_high_watermark_bytes", _memory_high_watermark_bytes);
    Json::get_to(json, "memory_low_watermark_bytes", _memory_low_watermark_bytes);

    // remove storage directory if it exists
    std::error_code ec;
    if (std::filesystem::exists(_disk_storage_dir)) {
        CHECK(std::filesystem::is_directory(_disk_storage_dir));
        std::filesystem::remove_all(_disk_storage_dir, ec);
        CHECK(!ec) << ec.message();
    }

    // create empty storage directory
    std::filesystem::create_directories(_disk_storage_dir, ec);
    CHECK(!ec) << ec.message();

    // setup RPC service
    nlohmann::json rpc_json;

    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("WriteCache RPC settings are not found");
    }

    _grpc_server_manager.init(rpc_json);
    _grpc_server_manager.addService(WriteCacheService::get_instance());
    _startup();
}

void
WriteCacheServer::add_extent(uint64_t db_id, uint64_t tid, uint64_t pg_xid, uint64_t lsn, const ExtentPtr data)
{
    WriteCacheIndexPtr index = get_index(db_id);

    uint64_t extent_size = data->byte_count();
    if (!_store_to_disk) {
        index->add_extent(tid, pg_xid, lsn, data, false);
        _add_memory(extent_size);
    } else {
        // add extent on disk
        index->add_extent(tid, pg_xid, lsn, data, true);
    }
}

void
WriteCacheServer::drop_table(uint64_t db_id, uint64_t tid, uint64_t pg_xid)
{
    WriteCacheIndexPtr index = get_index(db_id);

    uint64_t memory_removed{0};
    index->drop_table(tid, pg_xid, memory_removed);
    _subtract_memory(memory_removed);
}

void
WriteCacheServer::commit(uint64_t db_id, uint64_t xid, const std::vector<uint64_t>& pg_xids, WriteCacheTableSet::Metadata md)
{
    WriteCacheIndexPtr index = get_index(db_id);
    index->commit(pg_xids, xid, std::move(md));
}

void
WriteCacheServer::commit(uint64_t db_id, uint64_t xid, uint64_t pg_xid, WriteCacheTableSet::Metadata md)
{
    WriteCacheIndexPtr index = get_index(db_id);
    index->commit(pg_xid, xid, std::move(md));
}

void
WriteCacheServer::abort(uint64_t db_id, uint64_t pg_xid)
{
    WriteCacheIndexPtr index = get_index(db_id);

    uint64_t memory_removed{0};
    index->abort(pg_xid, memory_removed);
    _subtract_memory(memory_removed);
}

void
WriteCacheServer::abort(uint64_t db_id, const std::vector<uint64_t> &pg_xids)
{
    WriteCacheIndexPtr index = get_index(db_id);

    uint64_t memory_removed{0};
    index->abort(pg_xids, memory_removed);
    _subtract_memory(memory_removed);
}

std::vector<uint64_t>
WriteCacheServer::list_tables(uint64_t db_id, uint64_t xid, uint32_t count, uint64_t& cursor)
{
    WriteCacheIndexPtr index = get_index(db_id);

    std::vector<uint64_t> table_ids;

    auto &&tids = index->get_tids(xid, count, cursor);
    table_ids.reserve(tids.size());
    for (auto tid: tids) {
        table_ids.push_back(tid);
    }

    return table_ids;
}

std::vector<WriteCacheIndexExtentPtr>
WriteCacheServer::get_extents(uint64_t db_id, uint64_t tid, uint64_t xid, uint32_t count, uint64_t &cursor, WriteCacheTableSet::Metadata &md)
{
    WriteCacheIndexPtr index = get_index(db_id);

    std::vector<WriteCacheIndexExtentPtr> idx_extents =
        index->get_extents(tid, xid, count, cursor, md);

    return idx_extents;
}

void
WriteCacheServer::evict_xid(uint64_t db_id, uint64_t xid)
{
    WriteCacheIndexPtr index = get_index(db_id);

    uint64_t memory_removed{0};
    index->evict_xid(xid, memory_removed);
    _subtract_memory(memory_removed);
}

void
WriteCacheServer::evict_table(uint64_t db_id, uint64_t tid, uint64_t xid)
{
    WriteCacheIndexPtr index = get_index(db_id);

    uint64_t memory_removed{0};
    index->evict_table(tid, xid, memory_removed);
    _subtract_memory(memory_removed);
}

void
WriteCacheServer::drop_database(uint64_t db_id)
{
    WriteCacheIndexPtr index = get_index(db_id);

    uint64_t index_mem = index->get_memory_in_use();
    _indexes.erase(db_id);
    _subtract_memory(index_mem);
}

nlohmann::json
WriteCacheServer::get_memory_stats()
{
    std::shared_lock lock(_mem_mutex);
    nlohmann::json response = {
        {"high water mark", _memory_high_watermark_bytes},
        {"low water mark", _memory_low_watermark_bytes},
        {"current memory", _current_memory_bytes},
        {"store to disk", _store_to_disk.load()}
    };
    lock.unlock();
    response["databases"] = nlohmann::json::array();
    for (const auto &[db_id, index]: _indexes) {
        nlohmann::json database_data = {
            {"database id", db_id},
            {"current memory", index->get_memory_in_use()},
            {"partitions", index->get_partition_stats()}
        };
        response["databases"].push_back(database_data);
    }
    return response;
}

void
WriteCacheServer::_subtract_memory(uint64_t mem_size)
{
    std::unique_lock lock(_mem_mutex);
    _current_memory_bytes -= mem_size;
    if (_current_memory_bytes <= _memory_low_watermark_bytes) {
        _store_to_disk = false;
    }
}

void
WriteCacheServer::_add_memory(uint64_t mem_size)
{
    std::unique_lock lock(_mem_mutex);
    _current_memory_bytes += mem_size;
    if (_current_memory_bytes > _memory_high_watermark_bytes) {
        _store_to_disk = true;
    }
}

std::shared_ptr<WriteCacheIndex>
WriteCacheServer::get_index(uint64_t db_id)
{
    // upgrade_lock allows shared read initially, and can be upgraded to unique
    boost::upgrade_lock shared_lock(_db_mutex);

    // try to find the database and return it if it found
    auto it = _indexes.find(db_id);
    if (it != _indexes.end()) {
        return it->second;
    }

    // create storage directory for the database
    std::filesystem::path db_storage_dir = _disk_storage_dir / std::to_string(db_id);
    std::error_code ec;
    std::filesystem::create_directories(db_storage_dir, ec);
    CHECK(!ec) << ec.message();

    // Atomically upgrade to exclusive lock
    boost::upgrade_to_unique_lock unique_lock(shared_lock);

    // insert new database
    it = _indexes.emplace(db_id, std::make_shared<WriteCacheIndex>(db_storage_dir)).first;

    return it->second;
}

void
WriteCacheServer::_startup()
{
    _grpc_server_manager.startup();
}

void
WriteCacheServer::_internal_shutdown()
{
    _grpc_server_manager.shutdown();
    WriteCacheService::shutdown();
}

}  // namespace springtail
