#pragma once

#include <memory>
#include <string>

#include <common/singleton.hh>
#include <grpc/grpc_client.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <proto/sys_tbl_mgr.pb.h>
#include <storage/xid.hh>
#include <sys_tbl_mgr/schema_cache.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>
#include <proto/sys_tbl_mgr.grpc.pb.h>
#include <sys_tbl_mgr/shm_cache.hh>

namespace springtail::sys_tbl_mgr {

class Client : public Singleton<Client> {
    friend class Singleton<Client>;
public:
    void ping();

    std::string create_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);
    std::string alter_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);
    std::string drop_table(uint64_t db_id, const XidLsn &xid, const PgMsgDropTable &msg);

    std::string create_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg);
    std::string alter_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg);
    std::string drop_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg);


    std::string create_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg);
    std::string alter_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg);
    std::string drop_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg);

    std::string create_index(uint64_t db_id, const XidLsn &xid, const PgMsgIndex &msg, sys_tbl::IndexNames::State state);

    /**
     * Update state of the index on the SysTblMgr. The index must exist with the same xid.
     */
    void set_index_state(uint64_t db_id, const XidLsn &xid, uint64_t table_id, uint64_t index_id, sys_tbl::IndexNames::State state);

    /**
     * Call get_index_info() on the SysTblMgr.
     * @param tid The optional table ID that the index belongs to. Usually index ID's are unique is tid is optional
     *            There is a special case when tid is required. We construct primary indexes in create table
     *            using the column attributes and assign the same index ID=constant::PRIMARY_INDEX to all primary
     *            indexes and so tid is required for PRIMARY_INDEX.
     */
    proto::IndexInfo get_index_info(uint64_t db_id, uint64_t index_id, const XidLsn &xid, std::optional<uint64_t> tid = std::nullopt);
    std::string drop_index(uint64_t db_id, const XidLsn &xid, const PgMsgDropIndex &msg);

    void update_roots(uint64_t db_id, uint64_t table_id, uint64_t xid, const TableMetadata &metadata);
    void finalize(uint64_t db_id, uint64_t xid);
    void revert(uint64_t db_id, uint64_t xid);
    TableMetadataPtr get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid);

    std::shared_ptr<const SchemaMetadata> get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid);
    SchemaMetadataPtr get_target_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

    bool exists(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

    std::string create_namespace(const proto::NamespaceRequest &request);
    std::string swap_sync_table(const proto::NamespaceRequest &namespace_req,
                               const proto::TableRequest &create_req,
                               const std::vector<proto::IndexRequest> &index_reqs,
                               const proto::UpdateRootsRequest &roots_req);

    /** Create user defined type stub */
    std::string create_usertype(const proto::UserTypeRequest &request);

    /** Alter user defined type stub */
    std::string alter_usertype(const proto::UserTypeRequest &request);

    /** Drop user defined type stub */
    std::string drop_usertype(const proto::UserTypeRequest &request);

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

private:
    Client();
    ~Client() override = default;

    /** Pack the results into a SchemaMetadata. */
    SchemaMetadataPtr _pack_metadata(const proto::GetSchemaResponse &result);

    /** Cache for Schema objects. */
    std::shared_ptr<SchemaCache> _schema_cache;

    std::shared_ptr<grpc::Channel> _channel;
    std::unique_ptr<proto::SysTblMgr::Stub> _stub;
    std::atomic<std::shared_ptr<ShmCache>> _roots_cache;
};

} // namespace springtail::sys_tbl_mgr
