#include <mutex>
#include <stop_token>
#include <assert.h>
#include <algorithm>
#include <garbage_collector/indexer.hh>
#include <common/logging.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/client.hh>

namespace springtail::gc {

    Indexer::Indexer(uint32_t worker_count) 
    {
        assert(worker_count);
        for (auto i = 0; i != worker_count; ++i) {
            _workers.emplace_back([this](std::stop_token st) { task(st); });
        }
        SPDLOG_INFO("Indexer created: {}", worker_count);
    }

    void Indexer::process_ddls(uint64_t db_id, uint64_t xid, nlohmann::json const& ddls)
    {
        for (auto const& ddl: ddls) {
            auto action = ddl["action"];
            if (action == "create_index") {
                build({db_id, xid, ddl});
            } else if (action == "drop_index") {
                drop(db_id, ddl["id"], xid);
            } else {
                assert(false);
            }
        }
    }

    void Indexer::build(IndexParams idx)
    {
        {
            std::scoped_lock g(_m);
            Key key(idx._db_id, idx._ddl["id"]);
            // I don't think PG will issue two creates with the same index ID.
            assert(_work_set.find(key) == _work_set.end());
            _work_set[key] = std::move(idx);
            _queue.push(key);
        }
        // notify workers about new items
        _cv.notify_one();
    }

    void Indexer::drop(uint64_t db_id, uint64_t index_id, uint64_t xid)
    {
        std::scoped_lock g(_m);
        Key key(db_id, index_id);
        auto it = _work_set.find(key);
        if (it == _work_set.end()) {
            // note: the work item has _ddl.empty() == true
            // it means to drop the index
            _work_set[key] = {db_id, xid, {}};
            _queue.push(key);
            _cv.notify_one();
        } else {
            // TODO: we catch the case when the index is dropped in the middle b/c
            // the case isn't fully supported. The main problem is with managing
            // XID (finalize) updates.
            // The issues with XID will need to be resolved anyway for supporting 
            // asynchronous index updates.
            // Basically it would assert here if a single XID action contains
            // DDL's to create an index with index_id=1234.
            assert(false);
            // clear DDL, it will tell the worker to cancel the index build
            it->second._ddl = {};
        }
    }

    void Indexer::wait_for_completion(uint64_t db_id)
    {
        auto find_work = [&]() {
            auto it = std::ranges::find_if(_work_set,
                    [&](auto const& v) { 
                        return v.first.first == db_id;
                    });
            return it != _work_set.end();
        };

        // wait for the key {db, tid} to be removed from the working set
        std::unique_lock g(_m);
        while( find_work()  ) {
            _cv_done.wait(g, [&]{return !find_work();});
        }
    }

    void Indexer::wait_for_completion(uint64_t db_id, uint64_t tid)
    {
        auto find_work = [&]() {
            auto it = std::ranges::find_if(_work_set,
                    [&](auto const& v) { 
                        if (v.first.first == db_id && !v.second._ddl.is_null()) {
                            return v.second._ddl["table_id"] == tid;
                        }
                        return false;
                    });
            return it != _work_set.end();
        };

        // wait for the key {db, tid} to be removed from the working set
        std::unique_lock g(_m);
        while( find_work()  ) {
            _cv_done.wait(g, [&]{return !find_work();});
        }
    }

    void Indexer::task(std::stop_token st) 
    {
        while(!st.stop_requested()) {
            Key key;
            IndexParams params;

            // get the next work item
            {
                std::unique_lock g(_m);
                if (!_cv.wait(g, st, [&] { return !_queue.empty(); })) {
                    break;
                }

                key = _queue.front();
                _queue.pop();
                params = _work_set[key];
            }

            if (!params._ddl.is_null()) {
                auto root = _build(st, key, params);
                _commit_build(root, key, params);
            } else {
                _drop(st, key, params);
            }
        }
        SPDLOG_INFO("Indexer thread joined");
    }

    void Indexer::_drop(std::stop_token st, const Key& key, const IndexParams& idx)
    {
        assert(idx._ddl.is_null());

        auto [db_id, index_id] = key;
        SPDLOG_INFO("Drop index {}, {}, {}", db_id, index_id, idx._xid);

        auto client = sys_tbl_mgr::Client::get_instance();

        IndexParams work_item;
        {
            std::unique_lock g(_m);

            // fetch the latest state of the work item before we erase it
            work_item = _work_set[key];
            assert(work_item._ddl.is_null());

            _work_set.erase(key);
            _cv_done.notify_one();
        }

        XidLsn xid{idx._xid};
        sys_tbl_mgr::IndexInfo info = client->get_index_info(db_id, index_id, xid);
        if (info.id == 0) {
            //TODO: it seems like PG generates DROP INDEX with table ids, need
            //to investigate it more.
            SPDLOG_INFO("The index is not valid: {}", index_id);
            return;
        }

        SPDLOG_INFO("Drop index table id: {}", info.table_id);

        auto table = TableMgr::get_instance()->get_mutable_table(db_id, info.table_id, idx._xid, idx._xid);
        auto root = table->index(index_id);
        if (!root) {
            if (table->schema()->column_order().empty()) {
                // when dropping a table, PG generates DROP TABLE first
                // following by DROP INDEX. We ignore DROP INDEX after DROP TABLE.
                SPDLOG_INFO("Drop index not found: {}, {}", info.table_id, index_id);
                return;
            } else {
                assert(root);
            }
        }


        TableMetadata meta = client->get_roots(db_id, info.table_id, idx._xid);
        auto it = std::ranges::find_if(meta.roots, [&](auto const& v) {
                    return index_id == v.index_id;
                });
        assert(it != meta.roots.end());
        meta.roots.erase(it);
        client->update_roots(db_id, info.table_id, idx._xid, meta);

        root->truncate();
        root->finalize();

        SPDLOG_INFO("Index dropped: {}:{}", db_id, index_id);
    }

    MutableBTreePtr Indexer::_build(std::stop_token st, const Key& key, const IndexParams& idx)
    {
        constexpr int DROP_CHECK_PERIOD = 1000;

        SPDLOG_DEBUG_MODULE(LOG_GC, "Build index: {}:{} - {}", key.first, key.second, idx._ddl.dump());

        auto [db_id, index_id] = key;
        auto tid = idx._ddl["table_id"];

        assert(idx._ddl["action"] == "create_index");

        // index column positions
        std::vector<uint32_t> idx_cols;
        for (auto const& col: idx._ddl["columns"]) {
            idx_cols.push_back(col["position"]);
        }

        MutableBTreePtr root;
        std::shared_ptr<std::vector<FieldPtr>> key_fields;
        {
            auto table = TableMgr::get_instance()->get_mutable_table(db_id, tid, idx._xid, idx._xid);
            // create an index root
            root = table->create_index_root(index_id, idx_cols);
            root->init_empty();
            key_fields = table->schema()->get_fields(table->get_column_names(idx_cols));
        }

        // additional fields in the root schema to keep extent and row ids
        auto value_fields = std::make_shared<FieldArray>(2);
        uint64_t row_id = 0;

        auto table = TableMgr::get_instance()->get_table(db_id, tid, idx._xid);
        for (auto row_i = table->begin(); row_i != table->end(); ++row_i) {
            if(st.stop_requested()) {
                root->truncate();
                return {};
            }
            // check if the index was dropped
            if (row_id % DROP_CHECK_PERIOD == 0 && _was_dropped(key)) {
                return root;
            }
            auto extent_id = row_i.extent_id();

            (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);
            (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(static_cast<uint32_t>(row_id));

            // insert key
            auto &&svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, *row_i);
            root->insert(svalue);

            ++row_id;
        }
        SPDLOG_DEBUG_MODULE(LOG_GC, "Index build finished: {}:{}, rows={}", db_id, index_id, row_id);
        return root;
    }

    bool Indexer::_was_dropped(const Key& key) {
        std::unique_lock g(_m);
        auto const& params = _work_set[key];
        // index drop requested while we've been building it
        if (params._ddl.is_null()) {
            return true;
        }
        return false;
    }

    void Indexer::_commit_build(MutableBTreePtr root, const Key& key, const IndexParams& idx) 
    {
        if (!root) {
            //The build was cancelled as due to a shutdown.
            //When the system restarts it will check the redis precommit hash
            //and restart the build process.
            return;
        }

        auto [db_id, index_id] = key;
        auto tid = idx._ddl["table_id"];
        XidLsn xid{idx._xid};

        IndexParams work_item;

        {
            std::unique_lock g(_m);

            // fetch the latest state of the work item before we erase it
            work_item = _work_set[key];
            _work_set.erase(key);
            _cv_done.notify_one();
        }
        auto client = sys_tbl_mgr::Client::get_instance();
        if (!work_item._ddl.is_null()) {
            auto extent_id = root->finalize();
            auto&& meta = client->get_roots(db_id, tid, idx._xid);
            meta.roots.emplace_back(key.second, extent_id);
            client->update_roots(db_id, tid, idx._xid, meta);
        } else{
            // the index was deleted while we were building it
            root->truncate();
            //TODO: figure out out how to change the index state here to DELETED with the same XID
            assert(work_item._xid > idx._xid);
            XidLsn xid{work_item._xid};
            client->set_index_state(db_id, xid, tid, index_id, sys_tbl::IndexNames::State::DELETED);
        }
    }
}
