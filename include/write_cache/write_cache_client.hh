#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <grpc/grpc_client.hh>
#include <common/singleton.hh>
#include <common/timestamp.hh>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>
#include <proto/write_cache.grpc.pb.h>

namespace springtail {

class WriteCacheClient : public Singleton<WriteCacheClient> {
    friend class Singleton<WriteCacheClient>;

public:
    struct WriteCacheExtent {
        uint64_t xid;
        uint64_t lsn;
        std::string data;
    };

    /**
     * @brief Ping the server
     */
    void ping();

    /**
     * @brief Get a list of extents for a table at a given XID
     * @param db_id Database ID
     * @param tid Table ID
     * @param xid springtail XID
     * @param count Max. number of extents to fetch; done when count >= vector size
     * @param cursor In/Out cursor, in: set to 0 for start of range, out: current position
     * @param commit_ts out; postgres-reported commit ts of xid
     * @return std::vector<std::string>
     */
    std::vector<WriteCacheExtent> get_extents(uint64_t db_id,
                                              uint64_t tid,
                                              uint64_t xid,
                                              uint32_t count,
                                              uint64_t& cursor,
                                              PostgresTimestamp& commit_ts);

    /**
     * @brief Remove extents for a table at a given XID
     * @param db_id Database ID
     * @param tid Table ID
     * @param xid springtail XID
     */
    void evict_table(uint64_t db_id, uint64_t tid, uint64_t xid);

    /**
     * @brief Remove xid from cache and all data
     * @param db_id Database ID
     * @param xid springtail XID
     */
    void evict_xid(uint64_t db_id, uint64_t xid);

    /**
     * @brief List tables in write cache
     * @param db_id database ID
     * @param xid springtail XID
     * @param count max. number of tables to fetch; done when count >= vector size
     * @param cursor
     * @return std::vector<uint64_t>
     */
    std::vector<uint64_t> list_tables(uint64_t db_id,
                                      uint64_t xid,
                                      uint32_t count,
                                      uint64_t& cursor);

    //// TableExtentMapper API

    /**
     * @brief Add a GC-2 mapping to the extent mapper.  See TableExtentMapper::add_mapping().
     * @param db_id Database ID
     * @param tid Table ID
     * @param target_xid The XID at which the mapping occurred.
     * @param old_eid The extent ID that was modified.
     * @param new_eids The new extent IDs that were written to replace the old_eid.
     */
    void add_mapping(uint64_t db_id,
                     uint64_t tid,
                     uint64_t target_xid,
                     uint64_t old_eid,
                     const std::vector<uint64_t>& new_eids);

    /**
     * @brief Add a record of the use of and extent_id at a given XID within GC-1.  See
     *        TableExtentMapper::set_lookup().
     * @param db_id Database ID
     * @param tid Table ID
     * @param target_xid The XID at which the lookup occurred.
     * @param extent_id The extent ID that was referenced in the GC-1 lookup.
     */
    void set_lookup(uint64_t db_id, uint64_t tid, uint64_t target_xid, uint64_t extent_id);

    /**
     * @brief Maps the extent_id used at GC-1 lookup into the correct set of extent IDs that
     *        exist at the given XID given GC-2 mutations. See TableExtentMapper::forward_map().
     * @param db_id Database ID
     * @param tid Table ID
     * @param target_xid The XID at which the lookup occurred.
     * @param extent_id The extent ID that was referenced in the GC-1 lookup.
     */
    std::vector<uint64_t> forward_map(uint64_t db_id,
                                      uint64_t tid,
                                      uint64_t target_xid,
                                      uint64_t extent_id);

    /**
     * @brief Maps the extent_id found at a given XID to the set of extent IDs used in GC-1 to
     *        populate the write cache. Used by hurry-ups in the query nodes.  GC-2 doesn't need
     *        to do this since it knows its already applied all changes from the write
     *        cache. See TableExtentMapper::reverse_map().
     * @param db_id Database ID
     * @param tid Table ID
     * @param target_xid The XID at which the lookup occurred.
     * @param extent_id The extent ID that was referenced in the GC-1 lookup.
     */
    std::vector<uint64_t> reverse_map(
        uint64_t db_id, uint64_t tid, uint64_t access_xid, uint64_t target_xid, uint64_t extent_id);

    /**
     * @brief Clear any mappings stored against a given table up through the provided commit XID.
     * @param db_id Database ID
     * @param tid The table being expired.
     * @param commit_xid The XID at through which all changes have been applied.
     */
    void expire_map(uint64_t db_id, uint64_t tid, uint64_t commit_xid);

private:
    WriteCacheClient();
    ~WriteCacheClient() override = default;

    std::unique_ptr<proto::WriteCache::Stub> _stub;
    std::shared_ptr<grpc::Channel> _channel;
};

}  // namespace springtail
