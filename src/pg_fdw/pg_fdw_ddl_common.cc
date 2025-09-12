#include <pg_fdw/pg_fdw_ddl_common.hh>

namespace springtail::pg_fdw {
    std::string_view
    PgFdwCommon::_get_namespace_name(uint64_t db_id, uint64_t schema_xid, uint64_t namespace_id)
    {
        // lookup the namespace_id for the requested schema
        auto ns_table = TableMgr::get_instance()->get_table(db_id, sys_tbl::NamespaceNames::ID, schema_xid);
        auto ns_key = sys_tbl::NamespaceNames::Primary::key_tuple(namespace_id, schema_xid, constant::MAX_LSN);
        auto ns_i = ns_table->inverse_lower_bound(ns_key);

        if (ns_i == ns_table->end()) {
            LOG_WARN("Namespace not found {} @ {}:{}",
                        namespace_id, schema_xid, constant::MAX_LSN);
            return "";
        }

        auto ns_fields = ns_table->extent_schema()->get_fields();
        auto &&row = *ns_i;
        if (!ns_fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row)) {
            LOG_WARN("Namespace marked as not-exists {} @ {}:{}",
                        namespace_id, schema_xid, constant::MAX_LSN);
            return "";
        }

        return ns_fields->at(sys_tbl::NamespaceNames::Data::NAME)->get_text(&row);
    }

    std::pair<std::string_view, uint64_t>
    PgFdwCommon::_get_parent_table_info(uint64_t db_id, uint64_t schema_xid, uint64_t table_id)
    {
        auto table_names_t = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, schema_xid);
        auto schema = table_names_t->extent_schema();
        auto fields = schema->get_fields();

        auto search_key = sys_tbl::TableNames::Primary::key_tuple(table_id, schema_xid, constant::MAX_LSN);

        auto row_i = table_names_t->inverse_lower_bound(search_key);
        auto &&row = *row_i;

        // make sure table ID exists at this XID/LSN
        if (row_i == table_names_t->end() ||
            fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row) != table_id) {
            LOG_WARN("No table info at xid {}:{}", schema_xid, constant::MAX_LSN);
            return std::pair<std::string_view, uint64_t>();
        }

        // make sure that the table is marked as existing at this XID/LSN
        bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
        if (!exists) {
            LOG_WARN("Table marked non-existant at xid {}:{}", schema_xid, constant::MAX_LSN);
            return std::pair<std::string_view, uint64_t>();
        }

        return std::make_pair(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row),
                                fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(&row));
    }

    void
    PgFdwCommon::_iterate_table_names(uint64_t db_id,
                         uint64_t schema_xid,
                         uint64_t namespace_id,
                         bool exclude,
                         bool limit,
                         const std::set<std::string, std::less<>> &table_set,
                         [[maybe_unused]] const std::string_view namespace_name, // used only for logging
                         TableMap &table_map,
                         PartitionMap &table_partition_map)
    {
        // get the table names table to iterate over
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID,
                                                            schema_xid);
        // get field array
        auto fields = table->extent_schema()->get_fields();

        // iterate over the table names table and populate the table map
        for (auto row : (*table)) {
            auto table_ns_id = fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(&row);

            // check for schema-namespace match
            if (table_ns_id != namespace_id) {
                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Skipping row due to namespace mismatch {}, {}",
                                    table_ns_id, namespace_id);
                continue;
            }

            std::string table_name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row));
            // handle limit and exclude
            if (exclude && table_set.contains(table_name)) {
                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Excluding table {}.{}", namespace_name, table_name);
                continue;
            }

            // XXX should really stop after we have found all tables in limit
            if (limit && !table_set.contains(table_name)) {
                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Limit, skipping table {}.{}", namespace_name, table_name);
                continue;
            }

            uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
            uint64_t xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);

            if (xid > schema_xid) {
                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Table xid exceeds schema xid table {}.{} tid={}, xid={}, schema_xid={}",
                                    namespace_name, table_name, tid, xid, schema_xid);
                continue;
            }

            bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
            if (!exists) {
                // find table and compare xids, remove if this xid is >= to the one in the map
                auto entry = table_map.find(table_name);
                if (entry != table_map.end() && xid >= entry->second.xid) {
                    // remove this table entry
                    table_map.erase(entry);
                }
                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Removed non-existant table {}.{} tid={}, xid={}",
                                    namespace_name, table_name, tid, xid);
                continue;
            }

            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Found table {}.{} tid={}, xid={}", namespace_name, table_name, tid, xid);

            // lookup table in map, if found the xid if it is newer
            auto [it, inserted] = table_map.try_emplace(table_name, tid, xid, table_ns_id);

            // Insert the partition details in the partition map
            uint64_t parent_table_id = 0;
            if (!fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->is_null(&row)) {
                parent_table_id = fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->get_uint64(&row);
            }
            std::string partition_key = "";
            if (!fields->at(sys_tbl::TableNames::Data::PARTITION_KEY)->is_null(&row)) {
                partition_key = fields->at(sys_tbl::TableNames::Data::PARTITION_KEY)->get_text(&row);
            }
            std::string partition_bound = "";
            if (!fields->at(sys_tbl::TableNames::Data::PARTITION_BOUND)->is_null(&row)) {
                partition_bound = fields->at(sys_tbl::TableNames::Data::PARTITION_BOUND)->get_text(&row);
            }

            auto [tp_it, tp_inserted] = table_partition_map.try_emplace(
                tid,
                parent_table_id, partition_key, partition_bound
            );

            if (!inserted) {
                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Table {} already exists in schema {}", table_name, namespace_name);
                // update if xid is newer
                if (xid > it->second.xid) {
                    it->second = {tid, xid, table_ns_id};

                    // Update the table partition details as well
                    tp_it->second.set_parent_table_id(parent_table_id);
                    tp_it->second.set_partition_key(partition_key);
                    tp_it->second.set_partition_bound(partition_bound);
                }
            }
        }
    }
}
