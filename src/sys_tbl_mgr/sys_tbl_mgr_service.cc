#include <thrift/sys_tbl_mgr/Service.h> // generated file

#include <sys_tbl_mgr/sys_tbl_mgr_service.hh>
#include <sys_tbl_mgr/sys_tbl_mgr_server.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <storage/table_mgr.hh>
#include <storage/system_tables.hh>

namespace springtail::sys_tbl_mgr {
    /* static member initialization must happen outside of class */
    Service* Service::_instance {nullptr};
    boost::mutex Service::_instance_mutex;

    Service *
    Service::get_instance()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new Service();
        }

        return _instance;
    }

    void
    Service::shutdown()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    Service::Service()
    {
        // call into the XID Mgr to get the latest committed XID
        auto xid_mgr = XidMgrClient::get_instance();
        auto xid = xid_mgr->get_committed_xid();

        _access_xid = XidLsn(xid);
        _target_xid = xid + 1;
    }

    void
    Service::ping(Status& _return)
    {
        _return.__set_status(StatusCode::SUCCESS);
        _return.__set_message("PONG");
    }

    void
    Service::create_table(Status& _return,
                          const TableRequest &request)
    {
        // 1. acquire a shared lock to ensure no one is doing a finalize?
        boost::shared_lock lock(_write_mutex);
        XidLsn xid(request.xid, request.lsn);

        // 2. update the system tables with the relevant data

        // add table name
        auto table_info = std::make_shared<TableInfo>(request.table.id, request.xid, request.lsn,
                                                      request.table.schema, request.table.name, true);
        _set_table_info(table_info);

        // add roots and stats entry -- may get overwritten later if data is added to the table
        auto roots_info = std::make_shared<GetRootsResponse>();
        roots_info->roots.push_back(constant::UNKNOWN_EXTENT);
        _set_roots_info(request.table.id, xid, roots_info);

        // add schemas entries for each column
        std::vector<ColumnHistory> columns;
        std::map<uint32_t, uint32_t> primary_keys; // record the primary keys to update the indexes table
        for (const auto &column : request.table.columns) {
            ColumnHistory history;
            history.xid = xid.xid;
            history.lsn = xid.lsn;
            history.exists = true;
            history.update_type = static_cast<uint8_t>(SchemaUpdateType::NEW_COLUMN);
            history.column = column;

            columns.push_back(history);

            // record the primary key columns and order
            if (column.__isset.pk_position) {
                primary_keys[column.pk_position] = column.position;
            }
        }

        _set_schema_info(request.table.id, columns);

        // update the primary index information
        if (!primary_keys.empty()) {
            auto indexes_t = _get_mutable_system_table(sys_tbl::Indexes::ID);
            auto fields = sys_tbl::Indexes::Data::fields(request.table.id,
                                                         constant::INDEX_PRIMARY,
                                                         xid.xid,
                                                         xid.lsn,
                                                         0, // empty position; filled below
                                                         0); // empty column ID; filled below

            for (auto &&entry : primary_keys) {
                fields->at(sys_tbl::Indexes::Data::POSITION) = std::make_shared<ConstTypeField<uint32_t>>(entry.first);
                fields->at(sys_tbl::Indexes::Data::COLUMN_ID) = std::make_shared<ConstTypeField<uint32_t>>(entry.second);

                indexes_t->insert(std::make_shared<FieldTuple>(fields, nullptr),
                                  _target_xid, constant::UNKNOWN_EXTENT);
            }
        }

        //    f. XXX anything to do for secondary indexes?

        _return.__set_status(StatusCode::SUCCESS);
    }

    void
    Service::alter_table(Status& _return,
                         const TableRequest &request)
    {
        boost::shared_lock lock(_write_mutex);

        // retrieve the name of the table at the point of alteration
        XidLsn xid(request.xid, request.lsn);
        auto table_info = _get_table_info(request.table.id, xid);

        // note: table should always exist when calling alter_table()
        assert(table_info != nullptr);

        // if the name is changed, update the name in the table_names table
        if (table_info->schema != request.table.schema || table_info->name != request.table.name) {
            // insert the new name for this oid
            auto new_info = std::make_shared<TableInfo>(request.table.id,
                                                        request.xid,
                                                        request.lsn,
                                                        request.table.schema,
                                                        request.table.name,
                                                        true);
            _set_table_info(new_info);

        } else {
            XidLsn xid(request.xid, request.lsn);

            // get the schema prior to this change
            auto info = _get_schema_info(request.table.id, xid, xid);

            // generate a tuple for the change
            auto history = _generate_update(info->columns, request.table.columns, xid);

            // write the column change to the schemas table and update the cache
            _set_schema_info(request.table.id, { history });
        }

        _return.__set_status(StatusCode::SUCCESS);
    }

    void
    Service::drop_table(Status& _return,
                        const DropTableRequest &request)
    {
        boost::shared_lock lock(_write_mutex);

        // mark the table as dropped in the table_names
        auto table_info = std::make_shared<TableInfo>(request.table_id,
                                                      request.xid,
                                                      request.lsn,
                                                      request.schema,
                                                      request.name,
                                                      false);
        _set_table_info(table_info);

        // get the schema prior to this change
        XidLsn xid(request.xid, request.lsn);
        auto info = _get_schema_info(request.table_id, xid, xid);

        // remove all of the schema columns
        std::vector<ColumnHistory> changes;
        for (auto &column : info->columns) {
            ColumnHistory change;
            change.xid = request.xid;
            change.lsn = request.lsn;
            change.exists = false;
            change.update_type = static_cast<int8_t>(SchemaUpdateType::REMOVE_COLUMN);
            change.column = column;

            changes.push_back(change);
        }
        _set_schema_info(request.table_id, changes);

        _return.__set_status(StatusCode::SUCCESS);
    }

    void
    Service::update_roots(Status& _return,
                          const UpdateRootsRequest &request)
    {
        boost::shared_lock lock(_write_mutex);

        XidLsn xid(request.xid);

        auto info = std::make_shared<GetRootsResponse>();
        info->roots = request.roots;
        info->stats = request.stats;

        _set_roots_info(request.table_id, xid, info);

        _return.__set_status(StatusCode::SUCCESS);
    }

    void
    Service::finalize(Status& _return,
                      const FinalizeRequest &request)
    {
        // block all mutations
        boost::unique_lock wlock(_write_mutex);

        // finalize the mutated tables at the _target_xid
        std::map<uint64_t, std::vector<uint64_t>> roots;
        for (const auto &entry : _write) {
            roots[entry.first] = entry.second->finalize();
        }

        // block all read access while we swap access roots
        boost::unique_lock rlock(_read_mutex);

        // move the _access_xid to the _target_xid and update the access roots
        _access_xid = XidLsn(_target_xid);

        // move the _target_xid to just beyond the provided target xid
        assert(_target_xid <= request.xid);
        _target_xid = request.xid + 1;

        // note: we could update the table pointers?
        //       or maybe cache the roots to avoid reading the roots file?
        _read.clear();
        _write.clear();
        _clear_table_info();
        _clear_roots_info();
        _clear_schema_info();

        _return.__set_status(StatusCode::SUCCESS);
    }

    void
    Service::get_roots(GetRootsResponse& _return,
                       const GetRootsRequest &request)
    {
        boost::shared_lock lock(_read_mutex);

        XidLsn xid(request.xid);

        // make sure that the table exists at this XID
        auto table_info = _get_table_info(request.table_id, xid);
        if (table_info == nullptr) {
            return;
        }

        // get the roots
        auto info = _get_roots_info(request.table_id, xid);

        _return.__set_roots(info->roots);
        _return.__set_stats(info->stats);
    }

    void
    Service::get_schema(GetSchemaResponse& _return,
                        const GetSchemaRequest &request)
    {
        boost::shared_lock lock(_read_mutex);

        XidLsn xid(request.xid, request.lsn);
        auto info = _get_schema_info(request.table_id, xid, xid);
        
        _return = *info;
    }

    void
    Service::get_target_schema(GetSchemaResponse& _return,
                               const GetTargetSchemaRequest &request)
    {
        boost::shared_lock lock(_read_mutex);

        XidLsn access_xid(request.access_xid, request.access_lsn);
        XidLsn target_xid(request.target_xid, request.target_lsn);

        auto info = _get_schema_info(request.table_id, access_xid, target_xid);
        
        _return = *info;
    }

    Service::TableInfoPtr
    Service::_get_table_info(uint64_t table_id,
                                            const XidLsn &xid)
    {
        // check the cache
        auto table_i = _table_cache.find(table_id);
        if (table_i != _table_cache.end()) {
            // note: we keep XID/LSN in reverse order to allow use of lower_bound() for lookup
            auto info_i = table_i->second.lower_bound(xid);
            if (info_i != table_i->second.end()) {
                return info_i->second;
            }
        }

        // not present, read from disk
        auto table_names_t = _get_system_table(sys_tbl::TableNames::ID);
        auto schema = table_names_t->extent_schema();
        auto fields = schema->get_fields();

        auto search_key = sys_tbl::TableNames::Primary::key_tuple(table_id, xid.xid, xid.lsn);

        // find the row that matches the name of the table_id at the given XID/LSN
        auto row_i = table_names_t->inverse_lower_bound(search_key);

        // make sure table ID exists at this XID/LSN
        if (row_i == table_names_t->end() ||
            fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(*row_i) != table_id) {
            SPDLOG_WARN("No table info at xid {}:{}", xid.xid, xid.lsn);
            return nullptr;
        }

        // make sure that the table is marked as existing at this XID/LSN
        bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(*row_i);
        if (!exists) {
            SPDLOG_WARN("Table marked non-existant at xid {}:{}", xid.xid, xid.lsn);
            return nullptr;
        }

        // read the row from the extent and retrieve the FQN
        auto info = std::make_shared<TableInfo>();
        info->id = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(*row_i);
        info->xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(*row_i);
        info->lsn = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(*row_i);
        info->schema = fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(*row_i);
        info->name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(*row_i);
        info->exists = exists;

        // note: we currently only keep un-finalized mutations in the cache, so don't cache here
        return info;
    }

    void
    Service::_set_table_info(TableInfoPtr table_info)
    {
        XidLsn xid(table_info->xid, table_info->lsn);

        // update the cache
        _table_cache[table_info->id][xid] = table_info;

        // record the change to the system table
        auto table_names_t = _get_mutable_system_table(sys_tbl::TableNames::ID);
        auto tuple = sys_tbl::TableNames::Data::tuple(table_info->schema,
                                                      table_info->name,
                                                      table_info->id,
                                                      table_info->xid,
                                                      table_info->lsn,
                                                      table_info->exists);
        table_names_t->upsert(tuple, _target_xid, constant::UNKNOWN_EXTENT);
    }

    void
    Service::_clear_table_info()
    {
        // clear the table cache since it only contains un-finalized entries
        _table_cache.clear();
    }

    Service::RootsInfoPtr
    Service::_get_roots_info(uint64_t table_id,
                                            const XidLsn &xid)
    {
        // first check the cache
        auto roots_i = _roots_cache.find(table_id);
        if (roots_i != _roots_cache.end()) {
            auto info_i = roots_i->second.lower_bound(xid);
            if (info_i != roots_i->second.end()) {
                return info_i->second;
            }
        }

        auto roots_info = std::make_shared<GetRootsResponse>();

        // read from the tables
        auto roots_t = _get_system_table(sys_tbl::TableRoots::ID);
        auto roots_key_fields = roots_t->extent_schema()->get_sort_fields();

        auto search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id, constant::INDEX_PRIMARY, xid.xid);
        auto rrow_i = roots_t->inverse_lower_bound(search_key);

        // need to confirm that the table ID and index ID match, but the XID may not match
        if (rrow_i == roots_t->end()) {
            SPDLOG_WARN("Couldn't find table_roots entry for {}@{}:{} -- {}", table_id, xid.xid, xid.lsn, search_key->to_string());
            return roots_info;
        }

        auto table_id_f = roots_t->extent_schema()->get_field("table_id");
        auto index_id_f = roots_t->extent_schema()->get_field("index_id");
        if (table_id_f->get_uint64(*rrow_i) != table_id ||
            index_id_f->get_uint64(*rrow_i) != constant::INDEX_PRIMARY) {
            // no roots?  try to find it in the roots file by returning empty roots
            SPDLOG_WARN("Couldn't find table_roots entry for {}@{}:{} -- {} -- {},{}",
                        table_id, xid.xid, xid.lsn,
                        search_key->to_string(),
                        table_id_f->get_uint64(*rrow_i), index_id_f->get_uint64(*rrow_i));
            return roots_info;
        }

        // retrieve the root extent ID of the primary
        auto eid_f = roots_t->extent_schema()->get_field("extent_id");
        roots_info->roots.push_back(eid_f->get_uint64(*rrow_i));

        // get the root of the table's primary index
        auto stats_t = _get_system_table(sys_tbl::TableStats::ID);
        auto stats_key_fields = stats_t->extent_schema()->get_sort_fields();

        search_key = sys_tbl::TableStats::Primary::key_tuple(table_id, xid.xid);
        auto srow_i = stats_t->inverse_lower_bound(search_key);

        // need to confirm that the table ID matches, but the XID may not match
        table_id_f = stats_t->extent_schema()->get_field("table_id");
        if (srow_i == stats_t->end() || table_id_f->get_uint64(*srow_i) != table_id) {
            // no stats for this table?  seems like a potential error
            SPDLOG_WARN("Couldn't find table_stats entry for {}@{}:{}", table_id, xid.xid, xid.lsn);
            return roots_info;
        }

        // retrieve the stats from the row
        auto row_count_f = stats_t->extent_schema()->get_field("row_count");
        roots_info->stats.row_count = row_count_f->get_uint64(*srow_i);

        return roots_info;
    }

    void
    Service::_set_roots_info(uint64_t table_id,
                                            const XidLsn &xid,
                                            RootsInfoPtr roots_info)
    {
        // cache the roots info
        _roots_cache[table_id][xid] = roots_info;

        // update the table_roots
        auto table_roots_t = _get_mutable_system_table(sys_tbl::TableRoots::ID);
        for (int index_id = 0; index_id < roots_info->roots.size(); ++index_id) {
            uint64_t root = roots_info->roots[index_id];
            auto tuple = sys_tbl::TableRoots::Data::tuple(table_id, index_id, xid.xid, root);
            table_roots_t->upsert(tuple, _target_xid, constant::UNKNOWN_EXTENT);

            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Updated root {}@{}:{} {} - {}",
                                table_id, xid.xid, xid.lsn, index_id, roots_info->roots[index_id]);
        }

        // update the table_stats
        auto table_stats_t = _get_mutable_system_table(sys_tbl::TableStats::ID);
        auto tuple = sys_tbl::TableStats::Data::tuple(table_id, xid.xid, roots_info->stats.row_count);
        table_stats_t->upsert(tuple, _target_xid, constant::UNKNOWN_EXTENT);

        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Updated stats {}@{}:{} - {}",
                            table_id, xid.xid, xid.lsn, roots_info->stats.row_count);
    }

    void
    Service::_clear_roots_info()
    {
        // note: we clear everything because the cache only contains un-finalized data
        _roots_cache.clear();
    }

    Service::SchemaInfoPtr
    Service::_get_schema_info(uint64_t table_id,
                                             const XidLsn &access_xid,
                                             const XidLsn &target_xid)
    {
        auto info = std::make_shared<GetSchemaResponse>();

        // first read the columns from the schemas table
        XidLsn xid = std::min(access_xid, _access_xid);
        auto &&columns = _read_schema_columns(table_id, xid);

        // if the requested access XID is ahead of the on-disk XID, apply changes from the cache
        if (access_xid > _access_xid) {
            _apply_schema_cache_history(table_id, access_xid, columns);
        }

        // store the column data into the results
        for (auto &entry : columns) {
            info->columns.push_back(entry.second);
        }

        // note: at this point we have the set of columns at the access_xid
        if (access_xid == target_xid) {
            return info;
        }

        // now collect any history between access_xid and target_xid
        xid = std::min(_access_xid, target_xid);
        if (access_xid < xid) {
            // read any history from the on-disk table
            auto &&history = _read_schema_history(table_id, access_xid, xid);
            info->history.insert(info->history.end(), history.begin(), history.end());
        }

        xid = std::max(access_xid, _access_xid);
        if (target_xid > xid) {
            // read any history from the cache
            auto &&history = _get_schema_cache_history(table_id, xid, target_xid);
            info->history.insert(info->history.end(), history.begin(), history.end());
        }

        return info;
    }

    std::map<uint32_t, TableColumn>
    Service::_read_schema_columns(uint64_t table_id,
                                                 const XidLsn &access_xid)
    {
        std::map<uint32_t, TableColumn> columns;

        // get an accessor for the schema table
        auto schemas_t = _get_system_table(sys_tbl::Schemas::ID);

        // construct the column accessors for the schemas table
        auto schema = schemas_t->extent_schema();
        auto fields = schema->get_fields();

        // read everything with the given table_id
        auto search_key = sys_tbl::Schemas::Primary::key_tuple(table_id, 0, 0, 0);

        // find the valid column metadata for the provided access_xid
        auto table_i = schemas_t->lower_bound(search_key);
        for (; table_i != schemas_t->end(); ++table_i) {
            auto &row = *table_i;

            // get the table_id from the entry
            uint64_t tid = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(row);
            if (tid != table_id) {
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            // don't apply changes that are beyond the requested XID/LSN
            uint64_t xid = fields->at(sys_tbl::Schemas::Data::XID)->get_uint64(row);
            uint64_t lsn = fields->at(sys_tbl::Schemas::Data::LSN)->get_uint64(row);
            if (access_xid < XidLsn(xid, lsn)) {
                continue;
            }

            // remove the column if it doesn't exist
            auto position = fields->at(sys_tbl::Schemas::Data::POSITION)->get_uint32(row);
            bool exists = fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(row);
            if (!exists) {
                columns.erase(position);
            } else {
                // construct a column from the row
                TableColumn column;
                column.name = fields->at(sys_tbl::Schemas::Data::NAME)->get_text(row);
                column.type = fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(row);
                column.pg_type = fields->at(sys_tbl::Schemas::Data::PG_TYPE)->get_int32(row);
                column.position = position;
                column.is_nullable = fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(row);
                column.is_generated = false; // XXX
                if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(row)) {
                    column.default_value = fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(row);
                }
                // note: pk_position set via scan of Indexes system table later

                columns[position] = column;
            }
        }

        // if no schema (e.g., due to DROP TABLE) then return empty schema info
        if (columns.empty()) {
            return columns;
        }

        // retrieve the primary index data for the table at this XID/LSN
        auto indexes_t = _get_system_table(sys_tbl::Indexes::ID);

        schema = indexes_t->extent_schema();
        fields = schema->get_fields();

        // find the first entry that matches for this XID/LSN
        search_key = sys_tbl::Indexes::Primary::key_tuple(table_id, constant::INDEX_PRIMARY,
                                                          access_xid.xid, access_xid.lsn, 0);

        auto index_i = indexes_t->inverse_lower_bound(search_key);
        if (index_i == indexes_t->end()) {
            SPDLOG_WARN("Didn't find a primary index for the table: {}@{}:{}",
                        table_id, access_xid.xid, access_xid.lsn);
            return columns;
        }

        // determine the XID we found and only read those entries
        XidLsn index_xid(fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(*index_i),
                         fields->at(sys_tbl::Indexes::Data::LSN)->get_uint64(*index_i));

        for (; index_i != indexes_t->end(); ++index_i) {
            auto &row = *index_i;

            // ensure we are reading data for the requested table
            uint64_t tid = fields->at(sys_tbl::Indexes::Data::TABLE_ID)->get_uint64(row);
            if (tid != table_id) {
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            uint64_t xid = fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(row);
            uint64_t lsn = fields->at(sys_tbl::Indexes::Data::LSN)->get_uint64(row);

            // ensure we are still reading the correct XID/LSN
            if (index_xid != XidLsn(xid, lsn)) {
                break;
            }

            // update the primary key details in the schema columns
            uint32_t column_id = fields->at(sys_tbl::Indexes::Data::COLUMN_ID)->get_uint32(row);
            uint32_t index_pos = fields->at(sys_tbl::Indexes::Data::POSITION)->get_uint32(row);
            columns[column_id].pk_position = index_pos;
        }

        return columns;
    }

    void
    Service::_apply_schema_cache_history(uint64_t table_id,
                                         const XidLsn &xid,
                                         std::map<uint32_t, TableColumn> &columns)
    {
        // check the cache to see if it has entries for this table, if not, nothing to apply
        auto schema_i = _schema_cache.find(table_id);
        if (schema_i == _schema_cache.end()) {
            return;
        }

        // go through the history and apply any changes up through the provided XID/LSN
        for (auto &column : schema_i->second) {
            for (auto &history : column.second) {
                if (xid < XidLsn(history.xid, history.lsn)) {
                    break; // stop applying changes
                }

                // apply the recorded change
                if (history.exists) {
                    columns[history.column.position] = history.column;
                } else {
                    columns.erase(history.column.position);
                }
            }
        }
    }

    std::vector<ColumnHistory>
    Service::_read_schema_history(uint64_t table_id,
                                  const XidLsn &access_xid,
                                  const XidLsn &target_xid)
    {
        std::vector<ColumnHistory> history;

        // get an accessor for the schema table
        auto schemas_t = _get_system_table(sys_tbl::Schemas::ID);

        // construct the column accessors for the schemas table
        auto schema = schemas_t->extent_schema();
        auto fields = schema->get_fields();

        // read everything with the given table_id
        auto search_key = sys_tbl::Schemas::Primary::key_tuple(table_id, 0, 0, 0);

        // find the valid column metadata for the provided access_xid
        auto table_i = schemas_t->lower_bound(search_key);
        for (; table_i != schemas_t->end(); ++table_i) {
            auto &row = *table_i;

            // get the table_id from the entry
            uint64_t tid = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(row);
            if (tid != table_id) {
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            uint64_t xid = fields->at(sys_tbl::Schemas::Data::XID)->get_uint64(row);
            uint64_t lsn = fields->at(sys_tbl::Schemas::Data::LSN)->get_uint64(row);
            XidLsn row_xid(xid, lsn);

            // don't capture changes that are before the access_xid
            if (row_xid < access_xid) {
                continue;
            }

            // don't capture changes that are beyond the target_xid
            if (target_xid < row_xid) {
                continue;
            }

            // store the entry into the history
            ColumnHistory entry;
            entry.xid = xid;
            entry.lsn = lsn;
            entry.exists = fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(row);;
            entry.update_type = fields->at(sys_tbl::Schemas::Data::UPDATE_TYPE)->get_uint8(row);

            entry.column.name = fields->at(sys_tbl::Schemas::Data::NAME)->get_text(row);
            entry.column.type = fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(row);
            entry.column.pg_type = fields->at(sys_tbl::Schemas::Data::PG_TYPE)->get_int32(row);
            entry.column.position = fields->at(sys_tbl::Schemas::Data::POSITION)->get_uint32(row);
            entry.column.is_nullable = fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(row);
            entry.column.is_generated = false; // XXX
            if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(row)) {
                entry.column.default_value = fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(row);
            }

            history.push_back(entry);
        }

        return history;
    }

    std::vector<ColumnHistory>
    Service::_get_schema_cache_history(uint64_t table_id,
                                       const XidLsn &access_xid,
                                       const XidLsn &target_xid)

    {
        std::vector<ColumnHistory> history;

        // check the cache to see if it has entries for this table, if not, nothing to apply
        auto schema_i = _schema_cache.find(table_id);
        if (schema_i == _schema_cache.end()) {
            return history;
        }

        // go through the history and capture any changes up through the provided XID/LSN
        for (auto &column : schema_i->second) {
            for (auto &entry : column.second) {
                XidLsn xid(entry.xid, entry.lsn);

                if (xid < access_xid) {
                    continue; // already applied these changes
                }

                if (target_xid < xid) {
                    break; // stop capturing changes
                }

                history.push_back(entry);
            }
        }

        return history;
    }

    void
    Service::_set_schema_info(uint64_t table_id,
                              const std::vector<ColumnHistory> &columns)
    {
        std::map<uint32_t, uint32_t> primary_keys; // record the primary keys to update the indexes table
        auto schemas_t = _get_mutable_system_table(sys_tbl::Schemas::ID);

        // add the column change history to the cache
        for (auto &history : columns) {
            // XXX do we need to enforce XID ordering somehow here?  are we guaranteed to apply these in xid order?
            _schema_cache[table_id][history.column.position].push_back(history);

            // write the column data to the schemas table
            std::optional<std::string> value;
            if (history.column.__isset.default_value) {
                value = history.column.default_value;
            }
            auto tuple = sys_tbl::Schemas::Data::tuple(table_id,
                                                       history.column.position,
                                                       history.xid,
                                                       history.lsn,
                                                       history.exists,
                                                       history.column.name,
                                                       history.column.type,
                                                       history.column.pg_type, // pg type oid
                                                       history.column.is_nullable,
                                                       value,
                                                       history.update_type);

            schemas_t->upsert(tuple, _target_xid, constant::UNKNOWN_EXTENT);
        }
    }

    void
    Service::_clear_schema_info()
    {
        _schema_cache.clear();
    }

    TablePtr
    Service::_get_system_table(uint64_t table_id)
    {
        boost::unique_lock lock(_mutex);

        // check if we already have a copy of the table
        auto table_i = _read.find(table_id);
        if (table_i != _read.end()) {
            return table_i->second;
        }

        // otherwise create an interface to the table and cache it
        TablePtr table = TableMgr::get_instance()->get_table(table_id, _access_xid.xid, _access_xid.lsn);

        // cache the table interface
        _read[table_id] = table;
        return table;
    }

    MutableTablePtr
    Service::_get_mutable_system_table(uint64_t table_id)
    {
        // check if we already have the table open
        auto table_i = _write.find(table_id);
        if (table_i != _write.end()) {
            return table_i->second;
        }

        // otherwise create an interface to the table and cache it
        MutableTablePtr table = TableMgr::get_instance()->get_mutable_table(table_id, _access_xid.xid, _target_xid);

        // save the mutable table into the cache
        _write[table_id] = table;
        return table;
    }

    ColumnHistory
    Service::_generate_update(const std::vector<TableColumn> &old_schema,
                              const std::vector<TableColumn> &new_schema,
                              const XidLsn &xid)
    {
        ColumnHistory update;
        update.xid = xid.xid;
        update.lsn = xid.lsn;

        // if the old schema has more columns, then a column was removed
        if (old_schema.size() > new_schema.size()) {
            std::map<uint32_t, const TableColumn *> lookup;
            for (auto &column : new_schema) {
                lookup[column.position] = &column;
            }

            // find the missing column
            for (auto &&old_entry : old_schema) {
                auto &&new_i = lookup.find(old_entry.position);
                if (new_i == lookup.end()) {
                    // copy the old column details
                    update.column = old_entry;

                    // mark as a REMOVE_COLUMN update
                    update.update_type = static_cast<int8_t>(SchemaUpdateType::REMOVE_COLUMN);
                    update.exists = false;

                    return update;
                }
            }

            // we should have found a missing column
            assert(0);
        }

        // if the old schema has fewer columns, then a column was added
        if (old_schema.size() < new_schema.size()) {
            std::map<uint32_t, const TableColumn *> lookup;
            for (auto &column : old_schema) {
                lookup[column.position] = &column;
            }

            // find the missing column
            for (auto &new_entry : new_schema) {
                auto &&old_i = lookup.find(new_entry.position);
                if (old_i == lookup.end()) {
                    update.column = new_entry;

                    // generate an ADD_COLUMN update
                    update.update_type = static_cast<int8_t>(SchemaUpdateType::NEW_COLUMN);
                    update.exists = true;

                    return update;
                }
            }

            // we should have found an added column
            assert(0);
        }

        std::map<uint32_t, const TableColumn *> lookup;
        for (auto &column : new_schema) {
            lookup[column.position] = &column;
        }

        // otherwise, compare each column to find the one with the difference
        for (auto &entry : old_schema) {
            // find the same column in the new schema
            auto new_i = lookup.find(entry.position);
            auto &new_col = *(new_i->second);

            // it must exist or else the new and old schema are more than one modification apart
            assert(new_i != lookup.end());

            // check for differences
            if (entry.name != new_col.name) {
                // copy the new column details
                update.column = new_col;

                // mark them as a NAME_CHANGE update
                update.update_type = static_cast<int8_t>(SchemaUpdateType::NAME_CHANGE);
                update.exists = true;

                return update;
            }

            if (entry.is_nullable != new_col.is_nullable) {
                // copy the new column details
                update.column = new_col;

                // mark them as a NULLABLE_CHANGE update
                update.update_type = static_cast<int8_t>(SchemaUpdateType::NULLABLE_CHANGE);
                update.exists = true;

                return update;
            }

            if (entry.type != new_col.type) {
                // copy the new column details
                update.column = new_col;

                // mark them as a TYPE_CHANGE update
                update.update_type = static_cast<int8_t>(SchemaUpdateType::TYPE_CHANGE);
                update.exists = true;

                return update;
            }
        }

        // XXX can there be a schema change that results in no changes on the Springtail side?
        assert(false);
    }
}
