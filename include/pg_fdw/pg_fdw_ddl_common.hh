#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <vector>
#include <map>
#include <tuple>

#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/table_mgr_client.hh>

#include <common/libpq_connection.hh>

namespace springtail::pg_fdw {
    /** Partition info */
    class PartitionInfo {
    public:
        PartitionInfo(uint64_t parent_table_id,
                      std::string parent_namespace_name,
                      std::string parent_table_name,
                      std::string partition_key,
                      std::string partition_bound)
            : _parent_table_id(parent_table_id),
              _parent_namespace_name(std::move(parent_namespace_name)),
              _parent_table_name(std::move(parent_table_name)),
              _partition_key(std::move(partition_key)),
              _partition_bound(std::move(partition_bound)) {}

        PartitionInfo(uint64_t parent_table_id,
                      std::string partition_key,
                      std::string partition_bound)
            : _parent_table_id(parent_table_id),
              _partition_key(std::move(partition_key)),
              _partition_bound(std::move(partition_bound)) {}

        uint64_t parent_table_id() const { return _parent_table_id; }
        std::string_view parent_namespace_name() const { return _parent_namespace_name; }
        std::string_view parent_table_name() const { return _parent_table_name; }
        std::string_view partition_key() const { return _partition_key; }
        std::string_view partition_bound() const { return _partition_bound; }

        uint64_t set_parent_table_id(uint64_t parent_table_id) { return _parent_table_id = parent_table_id; }
        std::string_view set_parent_namespace_name(const std::string_view parent_namespace_name) { return _parent_namespace_name = parent_namespace_name; }
        std::string_view set_parent_table_name(const std::string_view parent_table_name) { return _parent_table_name = parent_table_name; }
        std::string_view set_partition_key(const std::string_view partition_key) { return _partition_key = partition_key; }
        std::string_view set_partition_bound(const std::string_view partition_bound) { return _partition_bound = partition_bound; }

    private:
        uint64_t _parent_table_id;
        std::string _parent_namespace_name;
        std::string _parent_table_name;
        std::string _partition_key;
        std::string _partition_bound;
    };

    class PgFdwCommon {
    public:
        struct TableEntry {
            uint64_t table_id;
            uint64_t xid;
            uint64_t namespace_id;
        };
        struct ColumnInfo {
            std::string column_name;
            std::string type_name;
            bool nullable;
        };
        using TableMap = std::map<std::string, TableEntry>;
        using PartitionMap = std::map<uint64_t, PartitionInfo>;
        using ColumnList = std::vector<ColumnInfo>;
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
                       const ColumnList &columns,
                       const PartitionMap &table_partition_map,
                       bool is_fdw,
                       Func escape_identifier)
        {
            const PartitionInfo &partition_info = table_partition_map.at(table_oid);

            bool is_regular_table = partition_info.parent_table_id() == 0 && partition_info.partition_key().empty();
            bool is_parent_partitioned_table = partition_info.parent_table_id() == 0 && !partition_info.partition_key().empty();
            bool is_leaf_partitioned_table = partition_info.parent_table_id() > 0 && !partition_info.partition_bound().empty() && partition_info.partition_key().empty();
            bool is_non_leaf_partitioned_table = partition_info.parent_table_id() > 0 && !partition_info.partition_bound().empty() && !partition_info.partition_key().empty();

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
                           const ColumnList &columns,
                           const PartitionInfo &partition_info,
                           bool is_foreign_table,
                           Func escape_identifier)
        {
            // no schema name needed
            std::string create = fmt::format("{} {}.{} \n",
                is_foreign_table ? "CREATE FOREIGN TABLE" : "CREATE TABLE",
                escape_identifier(namespace_name),
                escape_identifier(table));

            if (partition_info.parent_table_id() == 0) {
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
                create += fmt::format("\nPARTITION OF {}.{}", partition_info.parent_namespace_name(), partition_info.parent_table_name());
                create += fmt::format("\n{}", partition_info.partition_bound());
            }

            if (!partition_info.partition_key().empty()) {
                create += fmt::format("\nPARTITION BY {}", partition_info.partition_key());
            }
            if (is_foreign_table) {
                create += fmt::format("\nSERVER {} OPTIONS (tid '{}');", server_name, tid);
            } else {
                create += ";";
                create += fmt::format("\nCOMMENT ON TABLE {}.{} IS 'TID:{}';",
                    escape_identifier(namespace_name), escape_identifier(table), tid);
            }

            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Generated SQL: {}", create);

            return create;
        }

        /**
         * @brief Get the namespace info
         *
         * @param db_id database id
         * @param schema_xid schema xid
         * @param namespace_id namespace id
         * @return std::pair<std::string, uint64_t>
         */
        static std::string_view
        _get_namespace_name(uint64_t db_id, uint64_t schema_xid, uint64_t namespace_id);

        /**
         * @brief Get the parent table info
         *
         * @param db_id database id
         * @param schema_xid schema xid
         * @param table_id table id
         * @return std::pair<std::string_view, uint64_t>
         */
        static std::pair<std::string_view, uint64_t>
        _get_parent_table_info(uint64_t db_id, uint64_t schema_xid, uint64_t table_id);

        /**
         * @brief Iterate over the table names table and populate the table map
         *
         * @param db_id database id
         * @param schema_xid schema xid
         * @param namespace_id namespace id
         * @param exclude exclude flag
         * @param limit limit flag
         * @param table_set table set
         * @param namespace_name namespace name
         * @param table_map table map
         * @param table_partition_map table partition map
         */
        static void
        _iterate_table_names(uint64_t db_id,
                             uint64_t schema_xid,
                             uint64_t namespace_id,
                             bool exclude,
                             bool limit,
                             const std::set<std::string, std::less<>> &table_set,
                             [[maybe_unused]] const std::string_view namespace_name, // used only for logging
                             TableMap &table_map,
                             PartitionMap &table_partition_map);
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
                       const std::set<std::string, std::less<>> &table_set,
                       Func1 type_name_resolver,
                       Func2 escape_identifier,
                       bool is_fdw)
        {
            std::vector<std::string> commands;

            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Getting schema ddl for {} @ {}:{}", namespace_name, schema_xid, constant::MAX_LSN);

            // lookup the namespace_id for the requested schema
            auto ns_table = TableMgrClient::get_instance()->get_table(db_id, sys_tbl::NamespaceNames::ID, schema_xid);
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

            // map from table name -> <table id, xid, table_ns_id>
            TableMap table_map;
            PartitionMap table_partition_map;

            // iterate over the table names table and populate the table map
            _iterate_table_names(db_id, schema_xid, namespace_id, exclude, limit, table_set, namespace_name, table_map, table_partition_map);

            // reorganize the table_map to be from tid -> {xid, table}
            std::map<uint64_t, std::tuple<uint64_t, std::string, uint64_t>> tid_map;
            for (const auto &[table_name, table_info] : table_map) {
                tid_map[table_info.table_id] = {table_info.xid, table_name, table_info.namespace_id};
            }

            // Populate the parent table names for the partitioned tables
            for (auto &[partition_id, partition_info] : table_partition_map) {
                uint64_t parent_table_id = partition_info.parent_table_id();

                if (parent_table_id == 0) {
                    // not a partitioned table or its a parent table
                    continue;
                }
                auto parent_table = tid_map.find(parent_table_id);
                if (parent_table == tid_map.end()) {
                    // parent table not found, skip this partition
                    // Try getting the table from the system tables directly.
                    auto [parent_table_name, parent_table_namespace_id] = _get_parent_table_info(db_id, schema_xid, parent_table_id);
                    if (parent_table_namespace_id == 0) {
                        LOG_WARN("Parent table {} not found", parent_table_id);
                        continue;
                    }
                    partition_info.set_parent_namespace_name(_get_namespace_name(db_id, schema_xid, parent_table_namespace_id).data());
                    partition_info.set_parent_table_name(parent_table_name.data());
                } else {
                    partition_info.set_parent_namespace_name(_get_namespace_name(db_id, schema_xid, std::get<2>(parent_table->second)).data());
                    partition_info.set_parent_table_name(std::get<1>(parent_table->second));
                }
            }

            // Move on to iterating through the schemas table

            // column list: name, type, nullable
            ColumnList columns;

            uint64_t current_tid=0;
            std::string current_table;

            // get the schemas table
            auto table = TableMgrClient::get_instance()->get_table(db_id, sys_tbl::Schemas::ID,
                                                        schema_xid);

            // iterate through it
            auto fields = table->extent_schema()->get_fields();
            for (auto row : (*table)) {
                uint64_t tid = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(&row);
                uint64_t xid = fields->at(sys_tbl::Schemas::Data::XID)->get_uint64(&row);
                if (xid > schema_xid) {
                    continue;
                }

                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Found table in schemas table: {}, xid: {}, schema_xid: {}", tid, xid, schema_xid);

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
                        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Table {} not found in table map, skipping", tid);
                        continue;
                    }

                    // update current vars based on this tid and info from tid_map
                    current_tid = tid;
                    current_table = std::get<1>(it->second);
                }

                std::string column_name(fields->at(sys_tbl::Schemas::Data::NAME)->get_text(&row));
                bool exists = fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(&row);
                if (!exists) {
                    auto it = std::ranges::find_if(columns,
                    [&column_name](const ColumnInfo &column) {
                        return column.column_name == column_name;
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
            if (!columns.empty()) {
                std::string sql = _process_table(server_name, namespace_name, current_table, current_tid, columns, table_partition_map, is_fdw, escape_identifier);
                if (!sql.empty())
                    commands.push_back(sql);
            }

            return commands;
        }
    };
}
