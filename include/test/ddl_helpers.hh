#pragma once
#include <pg_repl/pg_repl_msg.hh>
#include <proto/sys_tbl_mgr.grpc.pb.h>
#include <sys_tbl_mgr/mutable_table.hh>

namespace springtail::test::ddl_helpers {
    void create_table(uint64_t db_id, uint64_t table_id, uint64_t xid, std::string table_name, std::vector<PgMsgSchemaColumn> columns);
    proto::IndexProcessRequest create_index(uint64_t db_id, uint64_t table_id, uint64_t xid, uint64_t index_id, std::string idx_name,
            std::vector<PgMsgSchemaColumn> columns, sys_tbl::IndexNames::State idx_state, bool is_unique=false, std::string index_type=std::string(constant::INDEX_TYPE_BTREE));
    void drop_index(uint64_t db_id, uint32_t index_id, uint64_t xid);
    void populate_table(MutableTablePtr mtable, const std::vector<std::vector<int32_t>>& data, bool is_update=false,
            uint64_t start_internal_row_id=1);
}
