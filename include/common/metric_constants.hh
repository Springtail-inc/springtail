#include <string_view>
#include <vector>

namespace springtail {
    // xid_mgr counter metrics
    constexpr std::string_view XID_MGR_RECORD_DDL_CHANGE_CALLS = "xid_mgr_record_ddl_change_calls";
    constexpr std::string_view XID_MGR_GET_PARTITION_CALLS = "xid_mgr_get_partition_calls";
    constexpr std::string_view XID_MGR_GET_COMMITTED_XID_CALLS = "xid_mgr_get_committed_xid_calls";
    constexpr std::string_view XID_MGR_COMMIT_XID_CALLS = "xid_mgr_commit_xid_calls";

    // xid_mgr histogram metrics
    constexpr std::string_view XID_MGR_COMMIT_XID_LATENCIES = "xid_mgr_commit_xid_latencies";
    constexpr std::string_view XID_MGR_RECORD_DDL_CHANGE_LATENCIES = "xid_mgr_record_ddl_change_latencies";

    // storage cache counter metrics
    constexpr std::string_view STORAGE_CACHE_GET_CALLS = "storage_cache_get_calls";
    constexpr std::string_view STORAGE_CACHE_GET_CACHE_MISSES = "storage_cache_get_cache_misses";
    constexpr std::string_view STORAGE_CACHE_PUT_CALLS = "storage_cache_put_calls";
    constexpr std::string_view STORAGE_CACHE_FLUSH_CALLS = "storage_cache_flush_calls";
    constexpr std::string_view STORAGE_CACHE_DROP_CALLS = "storage_cache_drop_calls";

    // storage cache histogram metrics
    constexpr std::string_view STORAGE_CACHE_FLUSH_LATENCIES = "storage_cache_flush_latencies";
    constexpr std::string_view STORAGE_CACHE_DROP_LATENCIES = "storage_cache_drop_latencies";

    // sys_tbl_mgr counter metrics
    constexpr std::string_view SYS_TBL_MGR_CREATE_INDEX_CALLS = "sys_tbl_mgr_create_index_calls";
    constexpr std::string_view SYS_TBL_MGR_DROP_INDEX_CALLS = "sys_tbl_mgr_drop_index_calls";
    constexpr std::string_view SYS_TBL_MGR_SET_INDEX_STATE_CALLS = "sys_tbl_mgr_set_index_state_calls";
    constexpr std::string_view SYS_TBL_MGR_GET_INDEX_INFO_CALLS = "sys_tbl_mgr_get_index_info_calls";
    constexpr std::string_view SYS_TBL_MGR_CREATE_TABLE_CALLS = "sys_tbl_mgr_create_table_calls";
    constexpr std::string_view SYS_TBL_MGR_ALTER_TABLE_CALLS = "sys_tbl_mgr_alter_table_calls";
    constexpr std::string_view SYS_TBL_MGR_DROP_TABLE_CALLS = "sys_tbl_mgr_drop_table_calls";
    constexpr std::string_view SYS_TBL_MGR_CREATE_NAMESPACE_CALLS = "sys_tbl_mgr_create_namespace_calls";
    constexpr std::string_view SYS_TBL_MGR_ALTER_NAMESPACE_CALLS = "sys_tbl_mgr_alter_namespace_calls";
    constexpr std::string_view SYS_TBL_MGR_DROP_NAMESPACE_CALLS = "sys_tbl_mgr_drop_namespace_calls";
    constexpr std::string_view SYS_TBL_MGR_UPDATE_ROOTS_CALLS = "sys_tbl_mgr_update_roots_calls";
    constexpr std::string_view SYS_TBL_MGR_SWAP_SYNC_TABLE_CALLS = "sys_tbl_mgr_swap_sync_table_calls";
    constexpr std::string_view SYS_TBL_MGR_FINALIZE_CALLS = "sys_tbl_mgr_finalize_calls";
    constexpr std::string_view SYS_TBL_MGR_GET_ROOTS_CALLS = "sys_tbl_mgr_get_roots_calls";
    constexpr std::string_view SYS_TBL_MGR_GET_SCHEMA_CALLS = "sys_tbl_mgr_get_schema_calls";
    constexpr std::string_view SYS_TBL_MGR_GET_TARGET_SCHEMA_CALLS = "sys_tbl_mgr_get_target_schema_calls";
    constexpr std::string_view SYS_TBL_MGR_EXISTS_CALLS = "sys_tbl_mgr_exists_calls";

    constexpr std::string_view PG_LOG_MGR_LOG_READER_LATENCIES = "pg_log_mgr_log_reader_latencies";

    namespace metrics {
        inline const std::vector<std::pair<std::string_view, std::string_view>> _counter_metrics = {
            // xid_mgr counter metrics
            {XID_MGR_RECORD_DDL_CHANGE_CALLS, "Total number of XID record DDL change calls"},
            {XID_MGR_GET_PARTITION_CALLS, "Total number of XID get partition calls"},
            {XID_MGR_GET_COMMITTED_XID_CALLS, "Total number of XID get committed xid calls"},
            {XID_MGR_COMMIT_XID_CALLS, "Total number of XID commit xid calls"},

            // storage cache counter metrics
            {STORAGE_CACHE_GET_CALLS, "Total number of storage cache get calls"},
            {STORAGE_CACHE_GET_CACHE_MISSES, "Total number of storage cache get cache misses"},
            {STORAGE_CACHE_PUT_CALLS, "Total number of storage cache put calls"},
            {STORAGE_CACHE_FLUSH_CALLS, "Total number of storage cache flush calls"},
            {STORAGE_CACHE_DROP_CALLS, "Total number of storage cache drop calls"},

            // sys_tbl_mgr counter metrics
            {SYS_TBL_MGR_CREATE_INDEX_CALLS, "Total number of sys_tbl_mgr create index calls"},
            {SYS_TBL_MGR_DROP_INDEX_CALLS, "Total number of sys_tbl_mgr drop index calls"},
            {SYS_TBL_MGR_SET_INDEX_STATE_CALLS, "Total number of sys_tbl_mgr set index state calls"},
            {SYS_TBL_MGR_GET_INDEX_INFO_CALLS, "Total number of sys_tbl_mgr get index info calls"},
            {SYS_TBL_MGR_CREATE_TABLE_CALLS, "Total number of sys_tbl_mgr create table calls"},
            {SYS_TBL_MGR_ALTER_TABLE_CALLS, "Total number of sys_tbl_mgr alter table calls"},
            {SYS_TBL_MGR_DROP_TABLE_CALLS, "Total number of sys_tbl_mgr drop table calls"},
            {SYS_TBL_MGR_CREATE_NAMESPACE_CALLS, "Total number of sys_tbl_mgr create namespace calls"},
            {SYS_TBL_MGR_ALTER_NAMESPACE_CALLS, "Total number of sys_tbl_mgr alter namespace calls"},
            {SYS_TBL_MGR_DROP_NAMESPACE_CALLS, "Total number of sys_tbl_mgr drop namespace calls"},
            {SYS_TBL_MGR_UPDATE_ROOTS_CALLS, "Total number of sys_tbl_mgr update roots calls"},
            {SYS_TBL_MGR_SWAP_SYNC_TABLE_CALLS, "Total number of sys_tbl_mgr swap sync table calls"},
            {SYS_TBL_MGR_FINALIZE_CALLS, "Total number of sys_tbl_mgr finalize calls"},
            {SYS_TBL_MGR_GET_ROOTS_CALLS, "Total number of sys_tbl_mgr get roots calls"},
            {SYS_TBL_MGR_GET_SCHEMA_CALLS, "Total number of sys_tbl_mgr get schema calls"},
            {SYS_TBL_MGR_GET_TARGET_SCHEMA_CALLS, "Total number of sys_tbl_mgr get target schema calls"},
            {SYS_TBL_MGR_EXISTS_CALLS, "Total number of sys_tbl_mgr exists calls"}
        };

        // histogram metrics
        inline const std::vector<std::pair<std::string_view, std::string_view>> _histogram_metrics = {
            // xid_mgr histogram metrics
            {XID_MGR_COMMIT_XID_LATENCIES, "Latency of XID commits"},
            {XID_MGR_RECORD_DDL_CHANGE_LATENCIES, "Latency of XID record DDL change calls"},

            // storage cache histogram metrics
            {STORAGE_CACHE_FLUSH_LATENCIES, "Latency of storage cache flush calls"},
            {STORAGE_CACHE_DROP_LATENCIES, "Latency of storage cache drop calls"},

            {PG_LOG_MGR_LOG_READER_LATENCIES, "Latency between when Postgres committed the transaction and when we process it in the log reader"}
        };
    }
}