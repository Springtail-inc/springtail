#include <string_view>
#include <vector>

namespace springtail {
    // xid_mgr counter metrics
    constexpr std::string_view XID_MGR_RECORD_DDL_CHANGE_CALLS = "xid_mgr_record_ddl_change_calls";
    constexpr std::string_view XID_MGR_GET_PARTITION_CALLS = "xid_mgr_get_partition_calls";
    constexpr std::string_view XID_MGR_GET_COMMITTED_XID_CALLS = "xid_mgr_get_committed_xid_calls";
    constexpr std::string_view XID_MGR_COMMIT_XID_CALLS = "xid_mgr_commit_xid_calls";

    // storage cache counter metrics
    constexpr std::string_view STORAGE_CACHE_GET_CALLS = "storage_cache_get_calls";
    constexpr std::string_view STORAGE_CACHE_GET_CACHE_MISSES = "storage_cache_get_cache_misses";
    constexpr std::string_view STORAGE_CACHE_PUT_CALLS = "storage_cache_put_calls";
    constexpr std::string_view STORAGE_CACHE_FLUSH_CALLS = "storage_cache_flush_calls";
    constexpr std::string_view STORAGE_CACHE_DROP_CALLS = "storage_cache_drop_calls";

    // storage cache histogram metrics
    constexpr std::string_view STORAGE_CACHE_FLUSH_LATENCIES = "storage_cache_flush_latencies";
    constexpr std::string_view STORAGE_CACHE_DROP_LATENCIES = "storage_cache_drop_latencies";

    // ingest histogram metrics
    /**
     * We collect the following time points.
     *
     * - Transaction BEGIN: ts_begin
     * - The committer finalizes the table: ts_finalized
     *
     * From these time points, we record the following latencies.
     *
     * TRANSACTION_LATENCIES = ts_finalized - ts_begin
     *
    */
    constexpr std::string_view LOG_READER_COMMIT_TXN_FREQ = "log_reader_commit_txn_freq";
    constexpr std::string_view LOG_READER_STREAM_ABORT_FREQ = "log_reader_stream_abort_freq";
    constexpr std::string_view LOG_READER_STREAM_COMMIT_FREQ = "log_reader_stream_commit_freq";

    constexpr std::string_view TRANSACTION_LATENCIES = "transaction_latencies";
    constexpr std::string_view WRITE_CACHE_FINALIZE_LATENCIES = "write_cache_finalize_latencies";

    constexpr std::string_view LOG_READER_QUEUE_SIZE = "log_reader_queue_size";
    constexpr std::string_view INGEST_MSG_QUEUE_SIZE = "ingest_msg_queue_size";
    constexpr std::string_view COMMITTER_QUEUE_SIZE = "committer_queue_size";

    constexpr std::string_view COMMITTER_TXN_MESSAGES = "committer_messages_per_txn";
    constexpr std::string_view COMMITTER_TXN_INSERTS = "committer_inserts";
    constexpr std::string_view COMMITTER_TXN_DELETES = "committer_deletes";
    constexpr std::string_view COMMITTER_TXN_UPDATES = "committer_updates";
    constexpr std::string_view COMMITTER_TXN_TRANCATES = "committer_trancates";

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

    // log manager histogram metrics
    constexpr std::string_view PG_LOG_MGR_LOG_READER_LATENCIES = "pg_log_mgr_log_reader_latencies";
    constexpr std::string_view PG_LOG_MGR_BTREE_LATENCIES = "pg_log_mgr_btree_write_latencies";


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
            // storage cache histogram metrics
            {STORAGE_CACHE_FLUSH_LATENCIES, "Latency of storage cache flush calls"},
            {STORAGE_CACHE_DROP_LATENCIES, "Latency of storage cache drop calls"},

            // log reader metrics
            {LOG_READER_COMMIT_TXN_FREQ, "COMMIT frequency"},
            {LOG_READER_STREAM_ABORT_FREQ, "Stream abort frequency"},
            {LOG_READER_STREAM_COMMIT_FREQ, "Stream commit frequency"},

            {TRANSACTION_LATENCIES, "From BEGIN to finalized tables"},
            {WRITE_CACHE_FINALIZE_LATENCIES, "Time takes for cache extents to be finalized"},

            {LOG_READER_QUEUE_SIZE, "Log reader queue size"},
            {INGEST_MSG_QUEUE_SIZE, "Message queue size"},
            {COMMITTER_QUEUE_SIZE, "Committer work queue size"},

            {COMMITTER_TXN_MESSAGES, "Messages per transaction"},
            {COMMITTER_TXN_INSERTS, "INSERT counter"},
            {COMMITTER_TXN_DELETES, "DELETE counter"},
            {COMMITTER_TXN_UPDATES, "UPDATE counter"},
            {COMMITTER_TXN_TRANCATES, "TRANCATE counter"},

            // log manager histogram metrics
            {PG_LOG_MGR_LOG_READER_LATENCIES, "Latency between when Postgres committed the transaction and when we process it in the log reader"},
            {PG_LOG_MGR_BTREE_LATENCIES, "Latency between postgres commit and btree write completion"}

        };



        /**
         * Storage cache counters.
         */
        struct StorageCache
        {
            struct GetCalls {
                static auto name() {
                    return STORAGE_CACHE_GET_CALLS;
                }
            };
            struct PutCalls {
                static auto name() {
                    return STORAGE_CACHE_PUT_CALLS;
                }
            };
            struct CacheMisses {
                static auto name() {
                    return STORAGE_CACHE_GET_CACHE_MISSES;
                }
            };
            struct FlushCalls {
                static auto name() {
                    return STORAGE_CACHE_FLUSH_CALLS;
                }
            };
            struct DropCalls {
                static auto name() {
                    return STORAGE_CACHE_DROP_CALLS;
                }
            };
        };
    }
}
