#pragma once

#include <memory>

#include <common/init.hh>

#include <grpc/grpc_client.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <proto/sys_tbl_mgr.pb.h>
#include <proto/sys_tbl_mgr.grpc.pb.h>
#include <storage/xid.hh>
#include <sys_tbl_mgr/schema_cache.hh>
#include <sys_tbl_mgr/shm_cache.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>

namespace springtail::sys_tbl_mgr {

class Client : public Singleton<Client>
{
    friend class Singleton<Client>;
public:
    void ping();

    TableMetadataPtr get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid);

    std::shared_ptr<const SchemaMetadata> get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid);
    SchemaMetadataPtr get_target_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

    bool exists(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

    /** Get user type at xid */
    std::shared_ptr<UserType> get_usertype(uint64_t db_id, uint64_t type_id, const XidLsn &xid);

    /**
     * Invalidates the schema entry for a given table from a given XID/LSN
     */
    void invalidate_table(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

    /**
     * Invalidates all of the tables of a db in the schema cache.
     */
    void invalidate_db(uint64_t db_id, const XidLsn &xid);

    /**
     * Provide the shared memory cache of roots for this client to use.
     */
    void use_roots_cache(std::shared_ptr<ShmCache> c);

    void invalidate_by_index(uint64_t db, uint64_t index_id, const XidLsn &xid)
    {
        _schema_cache->invalidate_by_index(db, index_id, xid);
    }


private:
    Client();
    ~Client() override = default;

    /** Cache for Schema objects. */
    std::shared_ptr<SchemaCache> _schema_cache;

    std::shared_ptr<grpc::Channel> _channel;
    std::unique_ptr<proto::SysTblMgr::Stub> _stub;
    std::atomic<std::shared_ptr<ShmCache>> _roots_cache;
};

} // namespace springtail::sys_tbl_mgr
