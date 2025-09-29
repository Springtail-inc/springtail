#pragma once

#include <pg_repl/pg_repl_msg.hh>
#include <proto/sys_tbl_mgr.grpc.pb.h>
#include <storage/schema.hh>
#include <storage/xid.hh>
#include <sys_tbl_mgr/table.hh>

namespace springtail::sys_tbl_mgr {
    class RequestHelper {
    public:

        /**
         * @brief Set common information. These data are a part of any request.
         *
         * @tparam Req
         * @param r - request
         * @param db_id - database id
         * @param xid - transaction id
         */
        template <typename Req>
        static void
        set_request_common(Req &r, uint64_t db_id, const XidLsn &xid)
        {
            r.set_db_id(db_id);
            r.set_xid(xid.xid);
            r.set_lsn(xid.lsn);
        }

        /**
         * @brief Set the partition information inside request
         *
         * @param info - partition data request
         * @param partition_data - partition information
         */
        static void
        set_partition_data(proto::PartitionData *info, const PartitionData &partition_data)
        {
            info->set_table_name(partition_data.table_name);
            info->set_table_id(partition_data.table_id);
            info->set_namespace_name(partition_data.namespace_name);
            info->set_namespace_id(partition_data.namespace_id);
            info->set_partition_bound(partition_data.partition_bound);
            info->set_partition_key(partition_data.partition_key);
            info->set_parent_table_id(partition_data.parent_table_id);
        }

        /**
         * @brief Generate table request.
         *
         * @param db_id - database id
         * @param xid - transaction id
         * @param msg - table message
         * @return proto::TableRequest - table request object
         */
        static proto::TableRequest
        gen_table_request(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg)
        {
            proto::TableRequest request;
            set_request_common(request, db_id, xid);

            auto *table = request.mutable_table();
            table->set_id(msg.oid);
            table->set_namespace_name(msg.namespace_name);
            table->set_name(msg.table);
            table->set_parent_table_id(msg.parent_table_id);
            table->set_partition_key(msg.partition_key);
            table->set_partition_bound(msg.partition_bound);
            table->set_rls_enabled(msg.rls_enabled);
            table->set_rls_forced(msg.rls_forced);

            for (const auto &col : msg.columns) {
                auto *column = table->add_columns();
                column->set_name(col.name);
                column->set_type(col.type);
                column->set_pg_type(col.pg_type);
                column->set_position(col.position);
                column->set_is_nullable(col.is_nullable);
                column->set_is_generated(col.is_generated);
                column->set_type_name(col.type_name);
                column->set_type_namespace(col.type_namespace);
                if (col.is_pkey) {
                    column->set_pk_position(col.pk_position);
                }
                if (col.default_value) {
                    column->set_default_value(*col.default_value);
                }
            }

            for (const auto &partition_data : msg.partition_data) {
                auto *info = table->add_partition_data();
                set_partition_data(info, partition_data);
            }

            return request;
        }

        /**
         * @brief Convert PgMsgSchemaColumn object into proto::TableColumn
         *
         * @param msg - schema column message
         * @return proto::TableColumn - table column in protobuf format
         */
        static proto::TableColumn
        get_table_column(const PgMsgSchemaColumn &msg)
        {
            proto::TableColumn column;
            column.set_name(msg.name);
            column.set_type(msg.type);
            column.set_pg_type(msg.pg_type);
            column.set_position(msg.position);
            column.set_is_nullable(msg.is_nullable);
            column.set_is_generated(msg.is_generated);
            column.set_type_name(msg.type_name);
            column.set_type_namespace(msg.type_namespace);
            if (msg.is_pkey) {
                column.set_pk_position(msg.pk_position);
            }
            if (msg.default_value) {
                column.set_default_value(*msg.default_value);
            }
            return column;
        }

        /**
         * @brief Generate index request.
         *
         * @param db_id - database id
         * @param xid - transaction id
         * @param msg - index message
         * @return proto::IndexRequest - index request object
         */
        static proto::IndexRequest
        gen_index_request(uint64_t db_id, const XidLsn &xid, const PgMsgIndex &msg)
        {
            proto::IndexRequest request;
            set_request_common(request, db_id, xid);
            auto *index = request.mutable_index();
            index->set_id(msg.oid);
            index->set_namespace_name(msg.namespace_name);
            index->set_name(msg.index);
            index->set_is_unique(msg.is_unique);
            index->set_table_name(msg.table_name);
            index->set_table_id(msg.table_oid);
            for (const auto &col : msg.columns) {
                auto *column = index->add_columns();
                column->set_position(col.position);
                column->set_name(col.name);
                column->set_idx_position(col.idx_position);
            }
            return request;
        }

        /**
         * @brief Convert API call result into schema metadata object
         *
         * @param result - result of an API call
         * @return SchemaMetadataPtr - schema metadata object
         */
        static SchemaMetadataPtr
        pack_metadata(const proto::GetSchemaResponse &result)
        {
            auto metadata = std::make_shared<SchemaMetadata>();
            for (const auto &column : result.columns()) {
                SchemaColumn value(column.name(), column.position(), static_cast<SchemaType>(column.type()),
                                column.pg_type(), column.is_nullable());
                if (column.has_pk_position()) {
                    value.pkey_position = column.pk_position();
                }
                if (column.has_default_value()) {
                    value.default_value = column.default_value();
                }

                metadata->columns.push_back(value);
            }
            // sort columns by position
            std::ranges::sort(metadata->columns,
                            [](auto const &a, auto const &b) { return a.position < b.position; });

            for (const auto &history : result.history()) {
                const auto &column = history.column();
                SchemaColumn value(history.xid(), history.lsn(), column.name(), column.position(),
                                static_cast<SchemaType>(column.type()), column.pg_type(),
                                history.exists(), column.is_nullable());
                value.update_type = static_cast<SchemaUpdateType>(history.update_type());
                if (column.has_pk_position()) {
                    value.pkey_position = column.pk_position();
                }
                if (column.has_default_value()) {
                    value.default_value = column.default_value();
                }

                metadata->history.push_back(value);
            }

            for (const auto &idx : result.indexes()) {
                Index info;
                info.id = idx.id();
                info.name = idx.name();
                info.schema = idx.namespace_name();
                info.state = idx.state();
                info.table_id = idx.table_id();
                info.is_unique = idx.is_unique();
                for (const auto &col : idx.columns()) {
                    info.columns.emplace_back(col.idx_position(), col.position());
                }
                // sort by index position
                std::ranges::sort(info.columns, [](auto const &a, auto const &b) {
                    return a.idx_position < b.idx_position;
                });
                metadata->indexes.push_back(std::move(info));
            }

            XidLsn access_start(result.access_xid_start(), result.access_lsn_start());
            XidLsn access_end(result.access_xid_end(), result.access_lsn_end());
            XidLsn target_start(result.target_xid_start(), result.target_lsn_start());
            XidLsn target_end(result.target_xid_end(), result.target_lsn_end());

            metadata->access_range = XidRange(access_start, access_end);
            metadata->target_range = XidRange(target_start, target_end);

            return metadata;
        }

        /**
         * @brief Convert API call result into table metadata object
         *
         * @param result - result of an API call
         * @return TableMetadataPtr - table metadata object
         */
        static TableMetadataPtr
        pack_table_metadata(const proto::GetRootsResponse &result)
        {
            auto metadata = std::make_shared<TableMetadata>();

            for (const auto &root : result.roots()) {
                metadata->roots.push_back({root.index_id(), root.extent_id()});
            }
            metadata->stats.row_count = result.stats().row_count();
            metadata->stats.end_offset = result.stats().end_offset();
            metadata->snapshot_xid = result.snapshot_xid();

            return metadata;
        }
    };

} // namespace springtail::sys_tbl_mgr

