#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <vector>
#include <map>
#include <tuple>

#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/table_mgr.hh>

#include <pg_repl/libpq_connection.hh>

namespace springtail::pg_fdw {
    /** Partition info */
    struct PartitionInfo {
        uint64_t parent_table_id;
        std::string parent_table_name;
        std::string partition_key;
        std::string partition_bound;
    };

    class PgFdwCommon {
    public:
        /**
         * @brief Helper to process a table and generate the query either for the FDW or the DDL manager
         *
         * @tparam Func Function to resolve the query to be executed
         * @param table_name table name
         * @param table_oid table OID
         * @param columns table columns
         * @param table_partition_map table partition map
         * @param is_fdw Flag to indicate if the output query is needed for FDW or the DDL manager
         * @return std::string
         */
        template<typename Func>
        static std::string
        _process_table(const std::string &server_name,
                       const std::string &namespace_name,
                       const std::string &table_name,
                       const uint64_t &table_oid,
                       const std::vector<std::tuple<std::string, std::string, bool>> &columns,
                       const std::map<uint64_t, PartitionInfo> &table_partition_map,
                       bool is_fdw,
                       Func escape_identifier)
        {
            const PartitionInfo &partition_info = table_partition_map.at(table_oid);

            bool is_regular_table = partition_info.parent_table_id == 0 && partition_info.partition_key.empty();
            bool is_parent_partitioned_table = partition_info.parent_table_id == 0 && !partition_info.partition_key.empty();
            bool is_leaf_partitioned_table = partition_info.parent_table_id > 0 && !partition_info.partition_bound.empty() && partition_info.partition_key.empty();
            bool is_non_leaf_partitioned_table = partition_info.parent_table_id > 0 && !partition_info.partition_bound.empty() && !partition_info.partition_key.empty();

            // Table Type                   - FDW       - DDL
            //
            // Regular Table                - CREATE    - SKIP
            // Parent Partitioned Table     - SKIP      - CREATE
            // Leaf Partitioned Table       - CREATE    - SKIP
            // Non-Leaf Partitioned Table   - SKIP      - CREATE

            if (is_regular_table || is_leaf_partitioned_table) {
                return is_fdw ? _gen_fdw_table_sql(server_name, namespace_name, table_name, table_oid, columns, partition_info, true, escape_identifier) : "";
            } else if (is_parent_partitioned_table || is_non_leaf_partitioned_table) {
                return is_fdw ? "" : _gen_fdw_table_sql(server_name, namespace_name, table_name, table_oid, columns, partition_info, false, escape_identifier);
            }
            return "";
        }

        /**
         * @brief Generate the SQL for a table to be created in the FDW. Depending on the partitioning, either create
         *        the table as a regular table or a foreign table
         *
         * @param server_name server name
         * @param namespace_name namespace name
         * @param table table name
         * @param tid table id
         * @param columns table columns
         * @param partition_info partition info
         * @param is_foreign_table flag to indicate if the table is a foreign table
         * @return std::string
         */
        template<typename Func>
        static std::string
        _gen_fdw_table_sql(const std::string &server_name,
                           const std::string &namespace_name,
                           const std::string &table,
                           uint64_t tid,
                           const std::vector<std::tuple<std::string, std::string, bool>> &columns,
                           const PartitionInfo &partition_info,
                           bool is_foreign_table,
                           Func escape_identifier)
        {
            // no schema name needed
            std::string create = fmt::format("{} {}.{} \n",
                is_foreign_table ? "CREATE FOREIGN TABLE" : "CREATE TABLE",
                escape_identifier(namespace_name),
                escape_identifier(table));

            if (partition_info.parent_table_id == 0) {
                create += " (";
                // iterate over the columns, adding each to the create statement
                // name, type, is_nullable, default value
                for (int i = 0; i < columns.size(); i++) {
                    const auto &[column_name, type_name, nullable] = columns[i];
                    std::string column = fmt::format(" {} ", escape_identifier(column_name));

                    // set the type name
                    column += type_name;

                    // add nullability and default
                    if (!nullable) {
                        column += " NOT NULL";
                    }

                    if (i < columns.size() - 1) {
                        column += ",\n";
                    }

                    create += column;
                }
                create += "\n)";
            } else {
                create += fmt::format("\nPARTITION OF {}.{}", namespace_name, partition_info.parent_table_name);
                create += fmt::format("\n{}", partition_info.partition_bound);
            }

            if (!partition_info.partition_key.empty()) {
                create += fmt::format("\nPARTITION BY {}", partition_info.partition_key);
            }
            if (is_foreign_table) {
                create += fmt::format("\nSERVER {} OPTIONS (tid '{}');", server_name, tid);
            } else {
                create += ";";
            }

            LOG_DEBUG(LOG_FDW, "Generated SQL: {}", create);

            return create;
        }

        /**
         * @brief Get the schema ddl object
         *
         * @tparam Func Function to resolve the column type name
         * @param db_id database id
         * @param schema_xid schema xid
         * @param server_name server name
         * @param namespace_name namespace name
         * @param exclude exclude flag
         * @param limit limit flag
         * @param table_set table set
         * @param type_name_resolver Function to resolve the column type name
         * @param is_fdw Flag to indicate if the output query is needed for FDW or the DDL manager
         * @return std::vector<std::string>
         */
        template <typename Func1, typename Func2>
        static std::vector<std::string>
        get_schema_ddl(uint64_t db_id,
                       uint64_t schema_xid,
                       const std::string &server_name,
                       const std::string &namespace_name,
                       bool exclude, bool limit,
                       const std::set<std::string> &table_set,
                       Func1 type_name_resolver,
                       Func2 escape_identifier,
                       bool is_fdw)
        {
            std::vector<std::string> commands;

            // lookup the namespace_id for the requested schema
            auto ns_table = TableMgr::get_instance()->get_table(db_id, sys_tbl::NamespaceNames::ID, schema_xid);
            auto ns_key = sys_tbl::NamespaceNames::Secondary::key_tuple(namespace_name, schema_xid, constant::MAX_LSN);
            auto ns_i = ns_table->inverse_lower_bound(ns_key, 1);

            // verify that the name is present and exists
            if (ns_i == ns_table->end(1)) {
                LOG_WARN("Couldn't find entry for namespace {} @ {}:{}",
                            namespace_name, schema_xid, constant::MAX_LSN);
                return commands;
            }

            auto ns_fields = ns_table->extent_schema()->get_fields();
            auto &&row = *ns_i;
            if (namespace_name != ns_fields->at(sys_tbl::NamespaceNames::Data::NAME)->get_text(&row)) {
                LOG_WARN("Couldn't find entry for namespace {} @ {}:{}",
                            namespace_name, schema_xid, constant::MAX_LSN);
                return commands;
            }
            if (!ns_fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row)) {
                LOG_WARN("Namespace marked as not-exists {} @ {}:{}",
                            namespace_name, schema_xid, constant::MAX_LSN);
                return commands;
            }

            // record the namespace ID
            uint64_t namespace_id = ns_fields->at(sys_tbl::NamespaceNames::Data::NAMESPACE_ID)->get_uint64(&row);

            // get the table names table to iterate over
            auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID,
                                                            schema_xid);
            // get field array
            auto fields = table->extent_schema()->get_fields();

            // map from table name -> <table id, xid>
            std::map<std::string, std::pair<uint64_t,uint64_t>> table_map;
            std::map<uint64_t, PartitionInfo> table_partition_map;

            // iterate over the table names table and populate the table map
            for (auto row : (*table)) {
                auto table_ns_id = fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(&row);

                // check for schema-namespace match
                if (table_ns_id != namespace_id) {
                    LOG_DEBUG(LOG_FDW, "Skipping row due to namespace mismatch {}, {}",
                                        table_ns_id, namespace_id);
                    continue;
                }

                std::string table_name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row));
                // handle limit and exclude
                if (exclude && table_set.contains(table_name)) {
                    LOG_DEBUG(LOG_FDW, "Excluding table {}.{}", namespace_name, table_name);
                    continue;
                }

                // XXX should really stop after we have found all tables in limit
                if (limit && !table_set.contains(table_name)) {
                    LOG_DEBUG(LOG_FDW, "Limit, skipping table {}.{}", namespace_name, table_name);
                    continue;
                }

                uint64_t tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
                uint64_t xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);

                bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
                if (!exists) {
                    // find table and compare xids, remove if this xid is >= to the one in the map
                    auto entry = table_map.find(table_name);
                    if (entry != table_map.end()) {
                        if (xid >= entry->second.second) {
                            // remove this table entry
                            table_map.erase(entry);
                        }
                    }
                    LOG_DEBUG(LOG_FDW, "Removed non-existant table {}.{} tid={}, xid={}",
                                        namespace_name, table_name, tid, xid);
                    continue;
                }

                LOG_DEBUG(LOG_FDW, "Found table {}.{} tid={}, xid={}", namespace_name, table_name, tid, xid);

                // lookup table in map, if found the xid if it is newer
                auto entry = table_map.insert({table_name, {tid, xid}});

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

                table_partition_map.try_emplace(
                    tid,
                    PartitionInfo(parent_table_id, "", partition_key, partition_bound)
                );

                if (entry.second == false) {
                    LOG_DEBUG(LOG_FDW, "Table {} already exists in schema {}", table_name, namespace_name);
                    // update if xid is newer
                    if (xid > entry.first->second.second) {
                        entry.first->second = {tid, xid};
                    }
                }
            }

            // reorganize the table_map to be from tid -> {xid, table}
            std::map<uint64_t, std::tuple<uint64_t, std::string>> tid_map;
            for (const auto &[table_name, table_info] : table_map) {
                tid_map[table_info.first] = {table_info.second, table_name};
            }

            // Populate the parent table names for the partitioned tables
            for (auto &partition_info : table_partition_map) {
                uint64_t parent_table_id = partition_info.second.parent_table_id;

                if (parent_table_id == 0) {
                    // not a partitioned table or its a parent table
                    continue;
                }
                auto parent_table = tid_map.find(parent_table_id);
                if (parent_table == tid_map.end()) {
                    // parent table not found, skip this partition
                    continue;
                }
                LOG_DEBUG(LOG_FDW, "Found parent table name {} for id: {}", std::get<1>(parent_table->second), parent_table_id);

                partition_info.second.parent_table_name = std::get<1>(parent_table->second);
            }

            // Move on to iterating through the schemas table

            // column list: name, type, nullable
            std::vector<std::tuple<std::string, std::string, bool>> columns;

            uint64_t current_tid=0;
            std::string current_table;

            // get the schemas table
            table = TableMgr::get_instance()->get_table(db_id, sys_tbl::Schemas::ID,
                                                        schema_xid);

            auto idx_table = TableMgr::get_instance()->get_table(db_id, sys_tbl::Indexes::ID,
                                                                schema_xid);

            auto idx_fields = idx_table->extent_schema()->get_fields();

            // iterate through it
            fields = table->extent_schema()->get_fields();
            for (auto row : (*table)) {
                uint64_t tid = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(&row);

                LOG_DEBUG(LOG_FDW, "Found table in schemas table: {}", tid);

                // check if we have moved to next tid
                if (tid != current_tid) {

                    if (!current_table.empty()) {
                        std::string sql = _process_table(server_name, namespace_name, current_table, current_tid, columns, table_partition_map, is_fdw, escape_identifier);
                        if (!sql.empty())
                            commands.push_back(sql);
                    }

                    // reset state
                    columns.clear();
                    current_table = "";

                    // do lookup of new tid in map
                    auto it = tid_map.find(tid);
                    if (it == tid_map.end()) {
                        // not found skip it
                        LOG_DEBUG(LOG_FDW, "Table {} not found in table map, skipping", tid);
                        continue;
                    }

                    // update current vars based on this tid and info from tid_map
                    current_tid = tid;
                    current_table = std::get<1>(it->second);
                }

                std::string column_name(fields->at(sys_tbl::Schemas::Data::NAME)->get_text(&row));
                bool exists = fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(&row);
                if (!exists) {
                    auto it = std::find_if(columns.begin(), columns.end(),
                    [&column_name](const std::tuple<std::string, std::string, bool> &column) {
                            return std::get<0>(column) == column_name;
                        });
                    if (it != columns.end()) {
                        columns.erase(it);
                    }
                    continue;
                }

                // add column if it exists
                int32_t pg_type(fields->at(sys_tbl::Schemas::Data::PG_TYPE)->get_int32(&row));
                bool nullable = fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(&row);

                columns.push_back({column_name, type_name_resolver(pg_type, namespace_id), nullable});
            }

            // process last table
            if (columns.size() > 0) {
                std::string sql = _process_table(server_name, namespace_name, current_table, current_tid, columns, table_partition_map, is_fdw, escape_identifier);
                if (!sql.empty())
                    commands.push_back(sql);
            }

            return commands;
        }
    };
}
