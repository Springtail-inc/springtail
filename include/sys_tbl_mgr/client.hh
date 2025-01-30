#pragma once

#include <mutex>
#include <memory>
#include <optional>
#include <string>

#include <common/singleton.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <storage/xid.hh>
#include <sys_tbl_mgr/schema_cache.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>

#include <thrift/sys_tbl_mgr/Service.h> // generated file
#include <thrift/common/thrift_client.hh>


namespace springtail::sys_tbl_mgr {

    class Client :
        public springtail::thrift::Client<Client, ServiceClient>,
        public Singleton<Client>
    {
        friend class Singleton<Client>;
    public:
        /**
         * @brief Ping the server
         */
        void ping();

        /**
         * Call create_table() on the SysTblMgr.
         */
        std::string create_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);

        /**
         * Call alter_table() on the SysTblMgr.
         */
        std::string alter_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);

        /**
         * Call drop_table() on the SysTblMgr.
         */
        std::string drop_table(uint64_t db_id, const XidLsn &xid, const PgMsgDropTable &msg);

        /**
         * Call create_namespace() on the SysTblMgr.
         */
        std::string create_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg);

        /**
         * Call alter_namespace() on the SysTblMgr.
         */
        std::string alter_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg);

        /**
         * Call drop_namespace() on the SysTblMgr.
         */
        std::string drop_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgDropNamespace &msg);

        /**
         * Call create_index() on the SysTblMgr.
         */
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
        IndexInfo get_index_info(uint64_t db_id, uint64_t index_id, const XidLsn &xid, std::optional<uint64_t> tid=std::nullopt);


        /**
         * Call drop_index() on the SysTblMgr.
         */
        std::string drop_index(uint64_t db_id, const XidLsn &xid, const PgMsgDropIndex &msg);

        /**
         * Call update_roots() on the SysTblMgr.
         */
        void update_roots(uint64_t db_id, uint64_t table_id, uint64_t xid,
                          const TableMetadata &metadata);

        /**
         * Call finalize() on the SysTblMgr.
         */
        void finalize(uint64_t db_id, uint64_t xid);

        /**
         * Call get_roots() on the SysTblMgr.
         */
        TableMetadataPtr get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid);

        /**
         * Call get_schema() on the SysTblMgr.
         */
        std::shared_ptr<const SchemaMetadata> get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Call get_target_schema() on the SysTblMgr.
         */
        SchemaMetadataPtr get_target_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

        /**
         * Call exists() on the SysTblMgr.
         */
        bool exists(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Call create_namespace() on the SysTblMgr.
         */
        std::string create_namespace(const NamespaceRequest &request);

        /**
         * Call swap_sync_table() on the SysTblMgr.
         */
        std::string swap_sync_table(const TableRequest &create, const UpdateRootsRequest &roots);

        /**
         * Invalidates the schema entry for a given table from a given XID/LSN
         */
        void invalidate_table(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Invalidates all of the tables of a db in the schema cache.
         */
        void invalidate_db(uint64_t db_id, const XidLsn &xid);

    private:
        /**
         * @brief Construct a new Write Cache Client object
         */
        Client();

        /**
         * @brief Destroy the Write Cache Client object; shouldn't be called directly use shutdown()
         */
        ~Client() override = default;

        /** Pack the results into a SchemaMetadata. */
        SchemaMetadataPtr _pack_metadata(const GetSchemaResponse &result);

        /** Cache for Schema objects. */
        std::shared_ptr<SchemaCache> _schema_cache;
    };

} // namespace springtail
