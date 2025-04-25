#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/client.hh>

namespace springtail::test::ddl_helpers {
    void create_table(uint64_t db_id, uint64_t table_id, uint64_t xid, std::vector<PgMsgSchemaColumn> columns)
    {
        // create a table
        PgMsgTable create_msg;
        create_msg.lsn = 0;
        create_msg.oid = table_id;
        create_msg.xid = xid;
        create_msg.namespace_name = "public";
        create_msg.table = "test_table";
        create_msg.columns = columns;

        TableMgr::get_instance()->create_table(db_id, { xid, 0 }, create_msg);

    }

    std::string create_index(uint64_t db_id, uint64_t table_id, uint64_t xid, uint64_t index_id,
            std::vector<PgMsgSchemaColumn> columns, sys_tbl::IndexNames::State idx_state)
    {

        PgMsgIndex msg;

        msg.lsn = 0;
        msg.xid = xid;
        msg.namespace_name = "public";
        msg.index = "secondary_index";
        msg.is_unique = false;
        msg.table_oid = table_id;
        msg.oid = index_id;

        int idx_position = 0;
        for(const auto& column: columns) {
            msg.columns.push_back({column.name, column.position, idx_position++});
        }

        XidLsn xid_lsn{xid};

        return sys_tbl_mgr::Client::get_instance()->create_index(db_id, xid_lsn, msg, idx_state);

    }

    void drop_index(uint64_t db_id, uint32_t index_id, uint64_t xid)
    {
        PgMsgDropIndex msg;

        msg.lsn = 0;
        msg.xid = xid;
        msg.namespace_name = "public";
        msg.oid = index_id;

        XidLsn xid_lsn{xid};

        sys_tbl_mgr::Client::get_instance()->drop_index(db_id, xid_lsn, msg);

        sys_tbl_mgr::Client::get_instance()->finalize(db_id, xid);
    }

    std::shared_ptr<Tuple>
        create_key(const std::string &name)
        {
            auto k = std::make_shared<ConstTypeField<std::string>>(name);
            std::vector<ConstFieldPtr> v({ k });
            return std::make_shared<ValueTuple>(v);
        }

    std::shared_ptr<Tuple>
        create_value(const std::vector<int32_t> &data)
        {
            std::vector<ConstFieldPtr> v;

            for (auto &d : data) {
                v.push_back(std::make_shared<ConstTypeField<int32_t>>(d));
            }

            return std::make_shared<ValueTuple>(v);
        }

    void populate_table(MutableTablePtr mtable, const std::vector<std::vector<int32_t>>& data)
    {
        // insert data to the tree
        for (int i = 0; i < data.size(); i++) {
            mtable->insert(create_value(data[i]), constant::UNKNOWN_EXTENT);
        }
    }
}
