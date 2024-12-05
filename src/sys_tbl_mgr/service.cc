#include "common/constants.hh"
#include "thrift/sys_tbl_mgr/sys_tbl_mgr_types.h"
#include <limits>
#include <stdexcept>
#include <thrift/sys_tbl_mgr/Service.h> // generated file

#include <sys_tbl_mgr/service.hh>
#include <sys_tbl_mgr/server.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/system_tables.hh>


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

    void
    Service::ping(Status& _return)
    {
        _return.__set_status(StatusCode::SUCCESS);
        _return.__set_message("PONG");
    }

    void
    Service::create_index(DDLStatement& _return,
                          const IndexRequest &request)
    {
        SPDLOG_INFO("got create_index()");

        // acquire a shared lock to ensure no one is doing a finalize
        boost::shared_lock lock(_write_mutex);

        // perform the CREATE INDEX
        auto &&ddl = _create_index(request);

        // serialize the JSON and return
        _return.__set_statement(nlohmann::to_string(ddl));
    }

    IndexInfo 
    Service::_get_index_info(const GetIndexInfoRequest &request)
    {
        XidLsn xid(request.xid, request.lsn);
        auto info = _find_index(request.db_id, request.index_id, xid);
        if (!info) {
            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Index not found: {}@{} - {}",
                            request.db_id, request.xid, request.index_id);
            IndexInfo dummy;
            dummy.id = 0;
            return dummy;
        }

        return info->first;
    }

    bool
    Service::_set_index_state(const SetIndexStateRequest &request)
    {
        XidLsn xid(request.xid, request.lsn);
        auto indexes = _read_schema_indexes(request.db_id, request.table_id, xid);

        auto index_i = std::ranges::find_if(indexes, [&](auto const& v) {
                return request.index_id == v.id; });

        if (index_i == indexes.end()) {
            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Index not found for table {} -- {}", request.table_id, request.index_id);
            return false;
        }

        // this is the existing index info
        auto index_info = *index_i;
        assert(index_info.table_id == request.table_id && index_info.id == request.index_id);

        auto index_names_t = _get_mutable_system_table(request.db_id, sys_tbl::IndexNames::ID);
        auto tuple = sys_tbl::IndexNames::Data::tuple(index_info.schema,
                index_info.name,
                index_info.table_id,
                request.index_id,
                xid.xid,
                xid.lsn,
                static_cast<sys_tbl::IndexNames::State>(request.state),
                index_info.is_unique );

        // update the index state
        index_names_t->insert(tuple, xid.xid, constant::UNKNOWN_EXTENT);

        // no need to carry the columns for deleted indexes
        if (request.state == static_cast<uint8_t>(sys_tbl::IndexNames::State::DELETED)) {
            return true;
        }

        std::map<uint32_t, uint32_t> keys;
        for (const auto &column : index_info.columns) {
            assert(keys.find(column.idx_position) == keys.end());
            keys[column.idx_position] = column.position;
        }

        // update columns with the state XID
        _write_index(xid, request.db_id, index_info.table_id, index_info.id, keys);

        return true;
    }

    void
    Service::set_index_state(Status& _return,
            const SetIndexStateRequest &request)
    {
        SPDLOG_INFO("got set_index_state()");

        // acquire a shared lock to ensure no one is doing a finalize
        boost::shared_lock lock(_write_mutex);

        if (_set_index_state(request)) {
            _return.__set_status(StatusCode::SUCCESS);
        } else {
            _return.__set_status(StatusCode::ERROR);
        }
    }

    void 
    Service::get_index_info(IndexInfo& _return, const GetIndexInfoRequest &request)
    {
        SPDLOG_INFO("got get_index_info()");

        // acquire a shared lock to ensure no one is doing a finalize
        boost::shared_lock lock(_read_mutex);

        _return = _get_index_info(request);
    }

    nlohmann::json
    Service::_create_index(const IndexRequest &request)
    {
        XidLsn xid(request.xid, request.lsn);

        nlohmann::json ddl;
        ddl["action"] = "create_index";
        ddl["schema"] = request.index.schema;
        ddl["index"] = request.index.name;
        ddl["id"] = request.index.id;
        ddl["is_unique"] = request.index.is_unique;
        ddl["table_id"] = request.index.table_id;
        ddl["columns"] = nlohmann::json::array();

        if (request.index.columns.empty()) {
            return ddl;
        }

        std::map<uint32_t, uint32_t> keys;
        for (const auto &column : request.index.columns) {
            assert(keys.find(column.idx_position) == keys.end());
            keys[column.idx_position] = column.position;

            // store the column data into the json
            nlohmann::json column_json;
            column_json["idx_position"] = column.idx_position;
            column_json["position"] = column.position;
            column_json["name"] = column.name;

            ddl["columns"].push_back(column_json);
        }

        // update index names
        {
            auto write_xid = _get_write_xid(request.db_id);
            auto index_names_t = _get_mutable_system_table(request.db_id, sys_tbl::IndexNames::ID);
            auto tuple = sys_tbl::IndexNames::Data::tuple(request.index.schema,
                    request.index.name,
                    request.index.table_id,
                    request.index.id,
                    xid.xid,
                    xid.lsn,
                    static_cast<sys_tbl::IndexNames::State>(request.index.state),
                    request.index.is_unique );

            index_names_t->insert(tuple, write_xid, constant::UNKNOWN_EXTENT);
        }

        _write_index(xid, request.db_id, request.index.table_id, request.index.id, keys);

        return ddl;
    }

    void
    Service::drop_index(DDLStatement& _return,
                          const DropIndexRequest &request)
    {
        SPDLOG_INFO("got drop_index()");

        // acquire a shared lock to ensure no one is doing a finalize
        boost::shared_lock lock(_write_mutex);


        nlohmann::json ddl;
        ddl["action"] = "drop_index";
        ddl["schema"] = request.schema;
        ddl["id"] = request.index_id;
        ddl["name"] = request.name;

        XidLsn xid(request.xid, request.lsn);

        // perform the CREATE INDEX
        _drop_index(xid, request.db_id, request.index_id);

        // serialize the JSON and return
        _return.__set_statement(nlohmann::to_string(ddl));
    }

    std::optional<std::pair<IndexInfo, XidLsn>> 
    Service::_find_index(uint64_t db_id, uint64_t index_id, const XidLsn& access_xid)
    {
        auto names_t = _get_system_table(db_id, sys_tbl::IndexNames::ID);
        auto names_schema = names_t->extent_schema();
        auto names_fields = names_schema->get_fields();

        auto search_key = sys_tbl::IndexNames::Primary::key_tuple(0, index_id, 0, 0);

        XidLsn index_xid;
        uint64_t table_id = 0;
        bool is_unique;
        std::string name;
        std::string schema;
        uint8_t state;
        bool found = false;

        // find the last XID for this index
        for (auto names_i = names_t->lower_bound(search_key); names_i != names_t->end(); ++names_i) {
            auto &row = *names_i;
            uint64_t id = names_fields->at(sys_tbl::IndexNames::Data::INDEX_ID)->get_uint64(row);

            if (id < index_id) {
                continue;
            }
            if (index_id != id) {
                SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "No data found for index {} -- {}", db_id, index_id);
                break;
            }

            {
                uint64_t xid = names_fields->at(sys_tbl::IndexNames::Data::XID)->get_uint64(row);
                uint64_t lsn = names_fields->at(sys_tbl::IndexNames::Data::LSN)->get_uint64(row);
                index_xid = {xid, lsn};
                if (access_xid < index_xid) {
                    continue;
                }
            }

            table_id = names_fields->at(sys_tbl::IndexNames::Data::TABLE_ID)->get_uint64(row);
            is_unique = names_fields->at(sys_tbl::IndexNames::Data::IS_UNIQUE)->get_bool(row);
            name = names_fields->at(sys_tbl::IndexNames::Data::NAME)->get_text(row);
            schema = names_fields->at(sys_tbl::IndexNames::Data::NAMESPACE)->get_text(row);
            state = names_fields->at(sys_tbl::IndexNames::Data::STATE)->get_uint8(row);
            found = true;
        }

        if (!found) {
            return {};
        }

        IndexInfo info;
        info.id = index_id;
        info.schema = schema;
        info.name = name;
        info.is_unique = is_unique;
        info.table_id = table_id;
        info.state = state;
        return {{info, index_xid}};
    }

    void
    Service::_drop_index(const XidLsn& xid, uint64_t db_id, uint64_t index_id)
    {
        auto names_t = _get_system_table(db_id, sys_tbl::IndexNames::ID);
        auto names_schema = names_t->extent_schema();
        auto names_fields = names_schema->get_fields();

        auto search_key = sys_tbl::IndexNames::Primary::key_tuple(0, index_id, 0, 0);

        //find the last record for the index id
        auto info = _find_index(db_id, index_id, {std::numeric_limits<decltype(xid.xid)>::max(), 0});

        if (!info) {
            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Drop index not found: {}@{} - {}",
                            db_id, xid.xid, index_id);
            return;
        }

        assert(xid > info->second);

        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Drop index found {}:{} -- {}", db_id, info->first.table_id, index_id);
        auto index_names_t = _get_mutable_system_table(db_id, sys_tbl::IndexNames::ID);
        auto tuple = sys_tbl::IndexNames::Data::tuple(info->first.schema,
                info->first.name,
                info->first.table_id,
                index_id,
                xid.xid,
                xid.lsn,
                sys_tbl::IndexNames::State::DELETED,
                info->first.is_unique );
        index_names_t->insert(tuple, xid.xid, constant::UNKNOWN_EXTENT);
    }

    void
    Service::create_table(DDLStatement& _return,
                          const TableRequest &request)
    {
        SPDLOG_INFO("got create_table()");

        // acquire a shared lock to ensure no one is doing a inalize
        boost::shared_lock lock(_write_mutex);

        // perform the CREATE TABLE
        auto &&ddl = _create_table(request);

        // serialize the JSON and return
        _return.__set_statement(nlohmann::to_string(ddl));
    }

    nlohmann::json
    Service::_create_table(const TableRequest &request)
    {
        XidLsn xid(request.xid, request.lsn);

        // initialize the ddl statement
        nlohmann::json ddl;
        ddl["action"] = "create";
        ddl["schema"] = request.table.schema;
        ddl["table"] = request.table.name;
        ddl["tid"] = request.table.id;
        ddl["columns"] = nlohmann::json::array();

        // add table name
        auto table_info = std::make_shared<TableInfo>(request.table.id, request.xid, request.lsn,
                                                      request.table.schema, request.table.name, true);
        _set_table_info(request.db_id, table_info);

        // add roots and stats entry -- may get overwritten later if data is added to the table
        auto roots_info = std::make_shared<GetRootsResponse>();
        sys_tbl_mgr::RootInfo ri;
        ri.index_id = constant::INDEX_PRIMARY;
        ri.extent_id = constant::UNKNOWN_EXTENT;
        roots_info->roots.push_back(ri);
        roots_info->stats.row_count = 0;
        roots_info->snapshot_xid = request.snapshot_xid;

        _set_roots_info(request.db_id, request.table.id, xid, roots_info);

        // add schemas entries for each column
        std::vector<ColumnHistory> columns;
        std::map<uint32_t, uint32_t> primary_keys; // record the primary keys to update the indexes table
        for (const auto &column : request.table.columns) {
            ColumnHistory history;
            history.xid = xid.xid;
            history.lsn = xid.lsn;
            history.exists = true;
            history.update_type = static_cast<uint8_t>(SchemaUpdateType::NEW_COLUMN);
            history.__set_column(column);

            columns.push_back(history);

            // record the primary key columns and order
            if (column.__isset.pk_position) {
                primary_keys[column.pk_position] = column.position;
            }

            // store the column data into the json
            nlohmann::json column_json;
            column_json["name"] = column.name;
            column_json["type"] = column.pg_type;
            column_json["nullable"] = column.is_nullable;
            if (column.__isset.default_value) {
                column_json["default"] = column.default_value;
            }

            ddl["columns"].push_back(column_json);
        }

        _set_schema_info(request.db_id, request.table.id, columns);

        // update the primary index information
        {
            auto write_xid = _get_write_xid(request.db_id);
            auto index_names_t = _get_mutable_system_table(request.db_id, sys_tbl::IndexNames::ID);
            auto tuple = sys_tbl::IndexNames::Data::tuple(request.table.schema,
                    request.table.name + ".primary_key",
                    request.table.id,
                    constant::INDEX_PRIMARY, //index id
                    xid.xid,
                    xid.lsn,
                    sys_tbl::IndexNames::State::READY,
                    true );
            index_names_t->insert(tuple, write_xid, constant::UNKNOWN_EXTENT);

            _write_index(xid, request.db_id, request.table.id, constant::INDEX_PRIMARY, primary_keys);
        }

        return ddl;
    }

    void
    Service::alter_table(DDLStatement& _return,
                         const TableRequest &request)
    {
        SPDLOG_INFO("got alter_table()");

        nlohmann::json ddl;
        ddl["tid"] = request.table.id;
        ddl["schema"] = request.table.schema;
        ddl["table"] = request.table.name;

        boost::shared_lock lock(_write_mutex);

        // retrieve the name of the table at the point of alteration
        XidLsn xid(request.xid, request.lsn);
        auto table_info = _get_table_info(request.db_id, request.table.id, xid);

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
            _set_table_info(request.db_id, new_info);

            // set the DDL statement
            ddl["action"] = "rename";
            ddl["old_table"] = table_info->name;
            ddl["old_schema"] = table_info->schema;

        } else {
            XidLsn xid(request.xid, request.lsn);

            // get the schema prior to this change
            auto info = _get_schema_info(request.db_id, request.table.id, xid, xid);

            // generate a tuple for the change
            // note: _generate_update() sets the necessary elements of the ddl
            auto history = _generate_update(info->columns, request.table.columns, xid, ddl);

            // we won't apply any changes to the system tables in these cases
            if (history.update_type != static_cast<int8_t>(SchemaUpdateType::NO_CHANGE) &&
                history.update_type != static_cast<int8_t>(SchemaUpdateType::RESYNC)) {

                // write the column change to the schemas table and update the cache
                _set_schema_info(request.db_id, request.table.id, { history });
            }
        }

        _return.__set_statement(nlohmann::to_string(ddl));
    }

    void
    Service::drop_table(DDLStatement& _return,
                        const DropTableRequest &request)
    {
        SPDLOG_INFO("got drop_table()");

        // hold a shared lock to prevent a concurrent finalize()
        boost::shared_lock lock(_write_mutex);

        // perform the DROP TABLE
        auto &&ddl = _drop_table(request);

        // serialize the ddl JSON and return
        _return.__set_statement(nlohmann::to_string(ddl));
    }

    nlohmann::json
    Service::_drop_table(const DropTableRequest &request)
    {
        // initialize the ddl json
        nlohmann::json ddl;
        ddl["action"] = "drop";
        ddl["tid"] = request.table_id;
        ddl["schema"] = request.schema;
        ddl["table"] = request.name;

        XidLsn xid(request.xid, request.lsn);

        // drop indexes
        
        auto indexes = _read_schema_indexes(request.db_id, request.table_id, xid);

        for (auto const& idx: indexes) {
            _drop_index(xid, request.db_id, idx.id); 
        }

        // mark the table as dropped in the table_names
        auto table_info = std::make_shared<TableInfo>(request.table_id,
                                                      request.xid,
                                                      request.lsn,
                                                      request.schema,
                                                      request.name,
                                                      false);
        _set_table_info(request.db_id, table_info);

        // get the schema prior to this change
        auto info = _get_schema_info(request.db_id, request.table_id, xid, xid);

        // remove all of the schema columns
        std::vector<ColumnHistory> changes;
        for (auto &column : info->columns) {
            ColumnHistory change;
            change.xid = request.xid;
            change.lsn = request.lsn;
            change.exists = false;
            change.update_type = static_cast<int8_t>(SchemaUpdateType::REMOVE_COLUMN);
            change.__set_column(column);

            changes.push_back(change);
        }
        _set_schema_info(request.db_id, request.table_id, changes);

        return ddl;
    }

    void
    Service::update_roots(Status& _return,
                          const UpdateRootsRequest &request)
    {
        SPDLOG_INFO("got update_roots()");

        // hold a shared lock to prevent a concurrent finalize()
        boost::shared_lock lock(_write_mutex);

        // update the metadata and return
        _update_roots(request);
        _return.__set_status(StatusCode::SUCCESS);
    }

    void
    Service::_update_roots(const UpdateRootsRequest &request)
    {
        XidLsn xid(request.xid);

        auto info = std::make_shared<GetRootsResponse>();
        info->roots = request.roots;
        info->stats = request.stats;
        info->snapshot_xid = request.snapshot_xid;

        _set_roots_info(request.db_id, request.table_id, xid, info);
    }

    XidLsn
    Service::_get_read_xid(uint64_t db_id)
    {
        boost::unique_lock lock(_xid_mutex);
        auto read_i = _read_xid.find(db_id);
        if (read_i != _read_xid.end()) {
            return read_i->second;
        }

        auto xid_mgr = XidMgrClient::get_instance();
        auto xid = xid_mgr->get_committed_xid(db_id, 0);

        _read_xid[db_id] = XidLsn(xid);
        _write_xid[db_id] = xid + 1;

        return XidLsn(xid);
    }

    uint64_t
    Service::_get_write_xid(uint64_t db_id)
    {
        boost::unique_lock lock(_xid_mutex);
        auto write_i = _write_xid.find(db_id);
        if (write_i != _write_xid.end()) {
            return write_i->second;
        }

        auto xid_mgr = XidMgrClient::get_instance();
        auto xid = xid_mgr->get_committed_xid(db_id, 0);

        _read_xid[db_id] = XidLsn(xid);
        _write_xid[db_id] = xid + 1;

        return xid + 1;
    }

    void
    Service::_set_xids(uint64_t db_id,
                       const XidLsn &read_xid,
                       uint64_t write_xid)
    {
        boost::unique_lock lock(_xid_mutex);
        _read_xid[db_id] = read_xid;
        _write_xid[db_id] = write_xid;
    }

    void
    Service::finalize(Status& _return,
                      const FinalizeRequest &request)
    {
        SPDLOG_INFO("got finalize()");

        // block all mutations
        boost::unique_lock wlock(_write_mutex);

        auto write_xid = _get_write_xid(request.db_id);
        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Finalize system tables: {}@{} >= {}",
                            request.db_id, request.xid, write_xid);

        // finalize the mutated tables at the write_xid
        // XXX we currently don't store the metadata, but re-read it from the roots file each time
        std::map<uint64_t, TableMetadata> md_map;
        for (const auto &entry : _write[request.db_id]) {
            md_map[entry.first] = entry.second->finalize();
        }
        if (md_map.empty()) {
            SPDLOG_INFO("Nothing to finalize: {}@{} >= {}",
                    request.db_id, request.xid, write_xid);
            return;
        }

        // block all read access while we swap access roots
        boost::unique_lock rlock(_read_mutex);

        // validate the current target XID against the requested XID
        assert(write_xid <= request.xid);

        // move the read_xid to the request xid, and move the write_xid to just beyond the
        // provided request xid
        _set_xids(request.db_id, XidLsn(request.xid), request.xid + 1);

        // note: we could update the table pointers?
        //       or maybe cache the roots to avoid reading the roots file?
        _read[request.db_id].clear();
        _write[request.db_id].clear();
        _clear_table_info(request.db_id);
        _clear_roots_info(request.db_id);
        _clear_schema_info(request.db_id);

        _return.__set_status(StatusCode::SUCCESS);
    }

    void
    Service::get_roots(GetRootsResponse& _return,
                       const GetRootsRequest &request)
    {
        SPDLOG_INFO("got get_roots()");

        boost::shared_lock lock(_read_mutex);

        XidLsn xid(request.xid, constant::MAX_LSN);

        // make sure that the table exists at this XID
        auto table_info = _get_table_info(request.db_id, request.table_id, xid);
        if (table_info == nullptr) {
            return;
        }

        // get the roots
        auto info = _get_roots_info(request.db_id, request.table_id, xid);

        _return.__set_roots(info->roots);
        _return.__set_stats(info->stats);
        _return.__set_snapshot_xid(info->snapshot_xid);
    }

    void
    Service::get_schema(GetSchemaResponse& _return,
                        const GetSchemaRequest &request)
    {
        SPDLOG_INFO("got get_schema()");

        boost::shared_lock lock(_read_mutex);

        XidLsn xid(request.xid, request.lsn);
        auto info = _get_schema_info(request.db_id, request.table_id, xid, xid);

        _return = *info;
    }

    void
    Service::get_target_schema(GetSchemaResponse& _return,
                               const GetTargetSchemaRequest &request)
    {
        SPDLOG_INFO("got get_target_schema() -- {}, {}", request.access_xid, request.target_xid);

        boost::shared_lock lock(_read_mutex);

        XidLsn access_xid(request.access_xid, request.access_lsn);
        XidLsn target_xid(request.target_xid, request.target_lsn);

        auto info = _get_schema_info(request.db_id, request.table_id, access_xid, target_xid);

        _return = *info;
    }

    bool
    Service::exists(const ExistsRequest &request)
    {
        SPDLOG_INFO("got exists()");

        boost::shared_lock lock(_read_mutex);

        XidLsn xid(request.xid, request.lsn);
        auto info = _get_table_info(request.db_id, request.table_id, xid);
        return (info != nullptr);
    }

    void
    Service::swap_sync_table(DDLStatement &_return,
                             const TableRequest &create,
                             const UpdateRootsRequest &roots)
    {
        SPDLOG_INFO("got swap_sync_table()");

        nlohmann::json ddls;

        // 1. acquire a shared lock to ensure no one is doing a finalize
        boost::shared_lock lock(_write_mutex);

        // 2. retrieve the table information at the end of the target XID
        XidLsn xid(create.xid, constant::MAX_LSN);
        auto info = _get_table_info(create.db_id, create.table.id, xid);

        // 3. if the table exists at the end of the XID, perform a drop
        if (info != nullptr) {
            DropTableRequest drop;
            drop.db_id = create.db_id;
            drop.table_id = create.table.id;
            drop.xid = create.xid;
            drop.lsn = create.lsn - 1;
            drop.schema = create.table.schema;
            drop.name = create.table.name;

            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Drop table: {}:{} @ {}:{}",
                                drop.db_id, drop.table_id, drop.xid, drop.lsn);

            auto &&drop_ddl = this->_drop_table(drop);
            ddls.push_back(drop_ddl);
        }

        // 4. perform a create table
        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Create table: {}:{} @ {}:{}",
                            create.db_id, create.table.id, create.xid, create.lsn);

        assert(create.lsn == constant::MAX_LSN - 1);
        auto &&create_ddl = this->_create_table(create);
        ddls.push_back(create_ddl);

        // 5. update the metadata of the table
        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Update roots: {}:{} @ {}:{}",
                            create.db_id, create.table.id, create.xid, create.lsn);
        this->_update_roots(roots);

        // 6. serialize the ddl json and return
        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Response: {}", nlohmann::to_string(ddls));
        _return.__set_statement(nlohmann::to_string(ddls));
    }


    Service::TableInfoPtr
    Service::_get_table_info(uint64_t db_id,
                             uint64_t table_id,
                             const XidLsn &xid)
    {
        // check the cache
        boost::unique_lock lock(_mutex);
        auto table_i = _table_cache[db_id].find(table_id);
        if (table_i != _table_cache[db_id].end()) {
            // note: we keep XID/LSN in reverse order to allow use of lower_bound() for lookup
            auto info_i = table_i->second.lower_bound(xid);
            if (info_i != table_i->second.end()) {
                return info_i->second;
            }
        }
        lock.unlock();

        // not present, read from disk
        auto table_names_t = _get_system_table(db_id, sys_tbl::TableNames::ID);
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
        info->lsn = fields->at(sys_tbl::TableNames::Data::LSN)->get_uint64(*row_i);
        info->schema = fields->at(sys_tbl::TableNames::Data::NAMESPACE)->get_text(*row_i);
        info->name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(*row_i);
        info->exists = exists;

        // note: we currently only keep un-finalized mutations in the cache, so don't cache here
        return info;
    }

    void
    Service::_set_table_info(uint64_t db_id,
                             TableInfoPtr table_info)
    {
        XidLsn xid(table_info->xid, table_info->lsn);

        // update the cache
        boost::unique_lock lock(_mutex);
        _table_cache[db_id][table_info->id][xid] = table_info;
        lock.unlock();

        // record the change to the system table
        auto write_xid = _get_write_xid(db_id);
        auto table_names_t = _get_mutable_system_table(db_id, sys_tbl::TableNames::ID);
        auto tuple = sys_tbl::TableNames::Data::tuple(table_info->schema,
                                                      table_info->name,
                                                      table_info->id,
                                                      table_info->xid,
                                                      table_info->lsn,
                                                      table_info->exists);
        table_names_t->upsert(tuple, write_xid, constant::UNKNOWN_EXTENT);
    }

    void
    Service::_clear_table_info(uint64_t db_id)
    {
        // clear the table cache since it only contains un-finalized entries
        boost::unique_lock lock(_mutex);
        _table_cache.erase(db_id);
    }

    Service::RootsInfoPtr
    Service::_get_roots_info(uint64_t db_id,
                             uint64_t table_id,
                             const XidLsn &xid)
    {
        // first check the cache
        boost::shared_lock lock(_mutex);
        auto roots_i = _roots_cache[db_id].find(table_id);
        if (roots_i != _roots_cache[db_id].end()) {
            auto info_i = roots_i->second.lower_bound(xid);
            if (info_i != roots_i->second.end()) {
                return info_i->second;
            }
        }
        lock.unlock();

        auto roots_info = std::make_shared<GetRootsResponse>();

        // read from the tables
        auto roots_t = _get_system_table(db_id, sys_tbl::TableRoots::ID);
        auto roots_key_fields = roots_t->extent_schema()->get_sort_fields();

        auto search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id, constant::INDEX_PRIMARY, 0);

        auto table_id_f = roots_t->extent_schema()->get_field("table_id");
        auto index_id_f = roots_t->extent_schema()->get_field("index_id");
        auto eid_f = roots_t->extent_schema()->get_field("extent_id");
        auto xid_f = roots_t->extent_schema()->get_field("xid");
        const std::string &sxid = sys_tbl::TableRoots::Data::SCHEMA[sys_tbl::TableRoots::Data::SNAPSHOT_XID].name;
        auto sxid_f = roots_t->extent_schema()->get_field(sxid);

        uint64_t snapshot_xid = 0;
        auto rrow_i = roots_t->lower_bound(search_key);

        for (; rrow_i != roots_t->end(); ++rrow_i) {
            if (table_id_f->get_uint64(*rrow_i) > table_id) {
                break;
            }
            if (table_id_f->get_uint64(*rrow_i) != table_id) {
                continue;
            }
            auto record_xid = xid_f->get_uint64(*rrow_i);
            if (xid.xid < record_xid) {
                continue;
            }
            sys_tbl_mgr::RootInfo ri;
            ri.index_id = index_id_f->get_uint64(*rrow_i);
            ri.extent_id = eid_f->get_uint64(*rrow_i);
            // use snapshot_xid of the last row
            snapshot_xid = sxid_f->get_uint64(*rrow_i);
            auto it = std::ranges::find_if(roots_info->roots, [&ri](auto const& v){ return v.index_id == ri.index_id; });
            if (it != roots_info->roots.end()) {
                // rrrow_i is ordered by xid, so the last record will be used
                it->extent_id =  ri.extent_id;
            } else {
                roots_info->roots.push_back(ri);
            }
        }

        if (roots_info->roots.empty()) {
            SPDLOG_WARN("Couldn't find table_roots entry for {}@{}:{} -- {}",
                        table_id, xid.xid, xid.lsn,
                        search_key->to_string());
            assert(0);
        }

        roots_info->snapshot_xid = snapshot_xid;


        // access the stats table
        auto stats_t = _get_system_table(db_id, sys_tbl::TableStats::ID);
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
    Service::_set_roots_info(uint64_t db_id,
                             uint64_t table_id,
                             const XidLsn &xid,
                             RootsInfoPtr roots_info)
    {
        // cache the roots info
        boost::unique_lock lock(_mutex);
        _roots_cache[db_id][table_id][xid] = roots_info;
        lock.unlock();

        // update the table_roots
        auto write_xid = _get_write_xid(db_id);
        auto table_roots_t = _get_mutable_system_table(db_id, sys_tbl::TableRoots::ID);
        for (auto const& r: roots_info->roots) {
            auto tuple = sys_tbl::TableRoots::Data::tuple(table_id, r.index_id, xid.xid, r.extent_id,
                                                          roots_info->snapshot_xid);
            table_roots_t->upsert(tuple, write_xid, constant::UNKNOWN_EXTENT);

            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Updated root {}@{}:{} {} - {}",
                                table_id, xid.xid, xid.lsn, r.index_id, r.extent_id);
        }

        // update the table_stats
        auto table_stats_t = _get_mutable_system_table(db_id, sys_tbl::TableStats::ID);
        auto tuple = sys_tbl::TableStats::Data::tuple(table_id, xid.xid, roots_info->stats.row_count);
        table_stats_t->upsert(tuple, write_xid, constant::UNKNOWN_EXTENT);

        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Updated stats {}@{}:{} - {}",
                            table_id, xid.xid, xid.lsn, roots_info->stats.row_count);
    }

    void
    Service::_clear_roots_info(uint64_t db_id)
    {
        // note: we clear everything because the cache only contains un-finalized data
        boost::unique_lock lock(_mutex);
        _roots_cache.erase(db_id);
    }

    Service::SchemaInfoPtr
    Service::_get_schema_info(uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn &access_xid,
                              const XidLsn &target_xid)
    {
        auto info = std::make_shared<GetSchemaResponse>();

        // first read the columns from the schemas table
        XidLsn &&read_xid = _get_read_xid(db_id);
        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Read schema info {}@{}:{} for @{}:{}",
                            table_id, access_xid.xid, access_xid.lsn, target_xid.xid, target_xid.lsn);

        // note: we always try to read data from disk up to the access_xid in case some of the data
        //       past the read_xid has already made it to disk
        auto &&columns = _read_schema_columns(db_id, table_id, access_xid);

        // if the requested access XID is ahead of the read XID, apply changes from the cache
        if (access_xid > read_xid) {
            _apply_schema_cache_history(db_id, table_id, access_xid, columns);
        }

        // store the column data into the results
        for (auto &entry : columns) {
            info->columns.push_back(entry.second);
        }

        info->indexes = _read_schema_indexes(db_id, table_id, access_xid);

        // note: at this point we have the set of columns at the access_xid
        if (access_xid == target_xid) {
            return info;
        }

        // now collect any history between access_xid and target_xid
        // note: we read any history from the on-disk table since there might always be some history
        //       on disk if the on-disk data is ahead of the read_xid
        auto &&history = _read_schema_history(db_id, table_id, access_xid, target_xid);
        info->history.insert(info->history.end(), history.begin(), history.end());

        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Tried to read history from disk: {}", info->history.size());

        // if the target is ahead of the guaranteed on-disk data then don't need to check the in-memory data
        XidLsn xid = std::max(access_xid, read_xid);
        if (target_xid > xid) {
            // read any history from the cache
            auto &&history = _get_schema_cache_history(db_id, table_id, xid, target_xid);
            info->history.insert(info->history.end(), history.begin(), history.end());

            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Tried to read history from memory: {}", info->history.size());
        }

        return info;
    }

    std::vector<IndexInfo> 
    Service::_read_schema_indexes(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid)
    {
        std::vector<IndexInfo> indexes;

        auto names_t = _get_system_table(db_id, sys_tbl::IndexNames::ID);
        auto names_schema = names_t->extent_schema();
        auto names_fields = names_schema->get_fields();

        auto indexes_t = _get_system_table(db_id, sys_tbl::Indexes::ID);
        auto indexes_schema = indexes_t->extent_schema();
        auto indexes_fields = indexes_schema->get_fields();

        auto search_key = sys_tbl::IndexNames::Primary::key_tuple(table_id, 0, 0, 0);

        for (auto names_i = names_t->lower_bound(search_key); names_i != names_t->end(); ++names_i) {
            auto &row = *names_i;
            uint64_t tid = names_fields->at(sys_tbl::IndexNames::Data::TABLE_ID)->get_uint64(row);

            if (tid != table_id) {
                SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "No more indexes for table {} -- {}", table_id, tid);
                break;
            }

            XidLsn index_xid;
            {
                uint64_t xid = names_fields->at(sys_tbl::IndexNames::Data::XID)->get_uint64(row);
                uint64_t lsn = names_fields->at(sys_tbl::IndexNames::Data::LSN)->get_uint64(row);
                index_xid = {xid, lsn};
                if (access_xid < index_xid) {
                    SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "No more data for table indexes {}@{}:{}", tid, xid, lsn);
                    continue;
                }
            }

            IndexInfo info;
            info.id = names_fields->at(sys_tbl::IndexNames::Data::INDEX_ID)->get_uint64(row);
            info.state = names_fields->at(sys_tbl::IndexNames::Data::STATE)->get_uint8(row);

            if (static_cast<sys_tbl::IndexNames::State>(info.state) == sys_tbl::IndexNames::State::DELETED) {
                SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Found deleted index {}@{}:{} - {}", tid, index_xid.xid, index_xid.lsn, info.id);
                // make sure to delete it from the result vector
                // note: DELETED will always come after or at the same XID of other states
                auto it = std::ranges::find_if(indexes, [&](auto const& v) {
                            return info.id == v.id; });
                if (it != indexes.end()) {
                    indexes.erase(it);
                }
                continue;
            }
            if (static_cast<sys_tbl::IndexNames::State>(info.state) == sys_tbl::IndexNames::State::NOT_READY) {
                SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Found not-ready index {}@{}:{} - {}", tid, index_xid.xid, index_xid.lsn, info.id);
            }

            info.name = names_fields->at(sys_tbl::IndexNames::Data::NAME)->get_text(row);
            info.schema = names_fields->at(sys_tbl::IndexNames::Data::NAMESPACE)->get_text(row);
            info.table_id = tid;
            info.is_unique = names_fields->at(sys_tbl::IndexNames::Data::IS_UNIQUE)->get_bool(row);

            auto index_key = sys_tbl::Indexes::Primary::key_tuple(table_id, info.id, index_xid.xid, index_xid.lsn, 0);
            for (auto index_i = indexes_t->lower_bound(index_key); index_i != indexes_t->end(); ++index_i) {
                auto &row = *index_i;
                uint64_t tid = indexes_fields->at(sys_tbl::Indexes::Data::TABLE_ID)->get_uint64(row);
                uint64_t index_id = indexes_fields->at(sys_tbl::Indexes::Data::INDEX_ID)->get_uint64(row);

                if (tid != table_id || index_id != info.id) {
                    SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "No more indexes for table {} -- {}, {} -- {}",
                            table_id, tid, index_id, info.id);
                    break;
                }
                // index_xid and xid's of index columns must match
                uint64_t xid = indexes_fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(row);
                uint64_t lsn = indexes_fields->at(sys_tbl::Indexes::Data::LSN)->get_uint64(row);
                if (index_xid != XidLsn(xid, lsn)) {
                    break;
                }

                IndexColumn col;
                col.position = indexes_fields->at(sys_tbl::Indexes::Data::COLUMN_ID)->get_uint32(row);
                col.idx_position = indexes_fields->at(sys_tbl::Indexes::Data::POSITION)->get_uint32(row);
                info.columns.push_back(std::move(col));
            }

            // erase any of the previous info, we'll keep the last one only
            auto it = std::ranges::find_if(indexes, [&](auto const& v) {
                    return info.id == v.id; });
            if (it != indexes.end()) {
                indexes.erase(it);
            }
            indexes.push_back(std::move(info));
        }

        return indexes;
    }

    std::map<uint32_t, TableColumn>
    Service::_read_schema_columns(uint64_t db_id,
                                  uint64_t table_id,
                                  const XidLsn &access_xid)
    {
        std::map<uint32_t, TableColumn> columns;

        // get an accessor for the schema table
        auto schemas_t = _get_system_table(db_id, sys_tbl::Schemas::ID);

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
                SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "No more data for table {} -- {}", table_id, tid);
                // if we have read all of the entries for this table ID, stop processing
                break;
            }

            // don't apply changes that are beyond the requested XID/LSN
            uint64_t xid = fields->at(sys_tbl::Schemas::Data::XID)->get_uint64(row);
            uint64_t lsn = fields->at(sys_tbl::Schemas::Data::LSN)->get_uint64(row);
            if (access_xid < XidLsn(xid, lsn)) {
                SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "No more data for table column {}@{}:{}", tid, xid, lsn);
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
                    column.__isset.default_value = true;
                }
                // note: pk_position set via scan of Indexes system table later

                columns[position] = column;
            }
        }

        // if no schema (e.g., due to DROP TABLE) then return empty schema info
        if (columns.empty()) {
            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Found no columns for table {}@{}:{}",
                                table_id, access_xid.xid, access_xid.lsn);
            return columns;
        }

        // retrieve the primary index data for the table at this XID/LSN
        auto indexes_t = _get_system_table(db_id, sys_tbl::Indexes::ID);

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

        bool done = false;
        while (!done) {
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
            columns[column_id].__set_pk_position(index_pos);

            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "Found index row {} for table {}@{}:{}",
                                column_id, table_id, access_xid.xid, access_xid.lsn);

            done = (index_pos == 0);
            if (!done) {
                --index_i;
            }
        }

        return columns;
    }

    void
    Service::_apply_schema_cache_history(uint64_t db_id,
                                         uint64_t table_id,
                                         const XidLsn &xid,
                                         std::map<uint32_t, TableColumn> &columns)
    {
        boost::shared_lock lock(_mutex);

        // check the cache to see if it has entries for this table, if not, nothing to apply
        auto schema_i = _schema_cache[db_id].find(table_id);
        if (schema_i == _schema_cache[db_id].end()) {
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
    Service::_read_schema_history(uint64_t db_id,
                                  uint64_t table_id,
                                  const XidLsn &access_xid,
                                  const XidLsn &target_xid)
    {
        std::vector<ColumnHistory> history;

        // get an accessor for the schema table
        auto schemas_t = _get_system_table(db_id, sys_tbl::Schemas::ID);

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
            entry.__set_column({});
            entry.column.name = fields->at(sys_tbl::Schemas::Data::NAME)->get_text(row);
            entry.column.type = fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(row);
            entry.column.pg_type = fields->at(sys_tbl::Schemas::Data::PG_TYPE)->get_int32(row);
            entry.column.position = fields->at(sys_tbl::Schemas::Data::POSITION)->get_uint32(row);
            entry.column.is_nullable = fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(row);
            entry.column.is_generated = false; // XXX
            if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(row)) {
                entry.column.default_value = fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(row);
                entry.column.__isset.default_value = true;
            }

            history.push_back(entry);
        }

        return history;
    }

    std::vector<ColumnHistory>
    Service::_get_schema_cache_history(uint64_t db_id,
                                       uint64_t table_id,
                                       const XidLsn &access_xid,
                                       const XidLsn &target_xid)

    {
        std::vector<ColumnHistory> history;
        boost::shared_lock lock(_mutex);

        // check the cache to see if it has entries for this table, if not, nothing to apply
        auto schema_i = _schema_cache[db_id].find(table_id);
        if (schema_i == _schema_cache[db_id].end()) {
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
    Service::_set_schema_info(uint64_t db_id,
                              uint64_t table_id,
                              const std::vector<ColumnHistory> &columns)
    {
        std::map<uint32_t, uint32_t> primary_keys; // record the primary keys to update the indexes table
        auto schemas_t = _get_mutable_system_table(db_id, sys_tbl::Schemas::ID);
        auto write_xid = _get_write_xid(db_id);

        // add the column change history to the cache
        for (auto &history : columns) {
            assert(history.__isset.column);
            assert(!history.__isset.index_column);

            // XXX do we need to enforce XID ordering somehow here?  are we guaranteed to apply these in xid order?
            boost::unique_lock lock(_mutex);
            _schema_cache[db_id][table_id][history.column.position].push_back(history);
            lock.unlock();

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
            schemas_t->upsert(tuple, write_xid, constant::UNKNOWN_EXTENT);
        }
    }

    void
    Service::_clear_schema_info(uint64_t db_id)
    {
        boost::unique_lock lock(_mutex);
        _schema_cache.erase(db_id);
    }

    TablePtr
    Service::_get_system_table(uint64_t db_id,
                               uint64_t table_id)
    {
        boost::unique_lock lock(_mutex);

        // check if we already have a copy of the table
        auto &cache = _read[db_id];
        auto table_i = cache.find(table_id);
        if (table_i != cache.end()) {
            return table_i->second;
        }

        // otherwise create an interface to the table and cache it
        auto &&read_xid = _get_read_xid(db_id);
        TablePtr table = TableMgr::get_instance()->get_table(db_id, table_id, read_xid.xid);

        // cache the table interface
        cache[table_id] = table;
        return table;
    }

    MutableTablePtr
    Service::_get_mutable_system_table(uint64_t db_id,
                                       uint64_t table_id)
    {
        boost::unique_lock lock(_mutex);

        // check if we already have the table open
        auto &cache = _write[db_id];
        auto table_i = cache.find(table_id);
        if (table_i != cache.end()) {
            return table_i->second;
        }

        // otherwise create an interface to the table and cache it
        auto &&read_xid = _get_read_xid(db_id);
        auto &&write_xid = _get_write_xid(db_id);
        MutableTablePtr table = TableMgr::get_instance()->get_mutable_table(db_id, table_id, read_xid.xid, write_xid);

        // save the mutable table into the cache
        cache[table_id] = table;
        return table;
    }

    void Service::_write_index(const XidLsn& xid, uint64_t db_id, uint64_t tab_id, uint64_t index_id, const std::map<uint32_t, uint32_t>& keys) {
        if (keys.empty()) {
            SPDLOG_INFO("The index has no keys: {}:{} - {}", db_id, tab_id, index_id);
            return;
        }
        auto write_xid = _get_write_xid(db_id);
        auto indexes_t = _get_mutable_system_table(db_id, sys_tbl::Indexes::ID);
        auto fields = sys_tbl::Indexes::Data::fields(tab_id,
                index_id,
                xid.xid,
                xid.lsn,
                0, // empty position; filled below
                0); // empty column ID; filled below

        for (auto &&entry : keys) {
            fields->at(sys_tbl::Indexes::Data::POSITION) = std::make_shared<ConstTypeField<uint32_t>>(entry.first);
            fields->at(sys_tbl::Indexes::Data::COLUMN_ID) = std::make_shared<ConstTypeField<uint32_t>>(entry.second);

            indexes_t->insert(std::make_shared<FieldTuple>(fields, nullptr),
                              write_xid, constant::UNKNOWN_EXTENT);
        }
    }

    ColumnHistory
    Service::_generate_update(const std::vector<TableColumn> &old_schema,
                              const std::vector<TableColumn> &new_schema,
                              const XidLsn &xid,
                              nlohmann::json &ddl)
    {
        ColumnHistory update;
        update.xid = xid.xid;
        update.lsn = xid.lsn;
        update.__set_column({});

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
                    update.__set_column(old_entry);

                    // mark as a REMOVE_COLUMN update
                    update.update_type = static_cast<int8_t>(SchemaUpdateType::REMOVE_COLUMN);
                    update.exists = false;

                    // set the DDL statement
                    ddl["action"] = "col_drop";
                    ddl["column"] = update.column.name;

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
                    // if we added a column with a default value, then we need to resync the entire
                    // table to get the new column data
                    if (new_entry.__isset.default_value) {
                        ddl["action"] = "resync";
                        update.update_type = static_cast<int8_t>(SchemaUpdateType::RESYNC);
                    } else {
                        // generate an ADD_COLUMN update
                        update.update_type = static_cast<int8_t>(SchemaUpdateType::NEW_COLUMN);
                        update.exists = true;
                        update.column = new_entry;

                        // set the DDL statement
                        ddl["action"] = "col_add";
                        ddl["column"]["name"] = update.column.name;
                        ddl["column"]["type"] = update.column.pg_type;
                        ddl["column"]["nullable"] = update.column.is_nullable;
                        if (update.column.__isset.default_value) {
                            ddl["column"]["default"] = update.column.default_value;
                        }
                    }

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

                // set the DDL statement
                ddl["action"] = "col_rename";
                ddl["old_name"] = entry.name;
                ddl["new_name"] = update.column.name;

                return update;
            }

            if (!entry.is_nullable && new_col.is_nullable) {
                // copy the new column details
                update.column = new_col;

                // mark them as a NULLABLE_CHANGE update
                update.update_type = static_cast<int8_t>(SchemaUpdateType::NULLABLE_CHANGE);
                update.exists = true;

                // set the DDL statement
                ddl["action"] = "col_nullable";
                ddl["column"]["name"] = update.column.name;
                ddl["column"]["nullable"] = update.column.is_nullable;

                return update;
            }

            if (entry.is_nullable && !new_col.is_nullable) {
                // a column going from nullable to not-nullable results in NULL values being
                // populated with a default, which aren't sent via the log
                ddl["action"] = "resync";
                update.update_type = static_cast<int8_t>(SchemaUpdateType::RESYNC);

                return update;
            }

            if (entry.pg_type != new_col.pg_type) {
                // a column type-change requires a table re-sync
                ddl["action"] = "resync";
                update.update_type = static_cast<int8_t>(SchemaUpdateType::RESYNC);

                return update;
            }

            if (entry.pk_position != new_col.pk_position) {
                // a primary key change requires a table re-sync
                ddl["action"] = "resync";
                update.update_type = static_cast<int8_t>(SchemaUpdateType::RESYNC);

                return update;
            }
        }

        // there may be changes to the schema that don't result in changes on the Springtail side,
        // e.g., a change in the default value
        ddl["action"] = "no_change";
        update.update_type = static_cast<int8_t>(SchemaUpdateType::NO_CHANGE);

        return update;
    }
}
