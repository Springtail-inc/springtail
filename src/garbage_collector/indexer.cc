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

    void Indexer::build(IndexParams idx)
    {
        {
            std::scoped_lock g(_m);
            Key key(idx._db_id, idx._ddl["id"]);
            // is it expected?
            assert(_work_set.find(key) == _work_set.end());
            _work_set[key] = std::move(idx);
            _queue.push(key);
        }
        // notify workers about new items
        _cv.notify_all();
    }

    void Indexer::drop(uint64_t db_id, uint64_t index_id, uint64_t xid)
    {
        std::scoped_lock g(_m);
        Key key(db_id, index_id);
        auto it = _work_set.find(key);
        if (it == _work_set.end()) {
            // note: the work item has _ddl.empty() == true
            // it means to drop the index
            _work_set[key] = {xid, {}};
            _queue.push(key);
            _cv.notify_one();
        } else {
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
//TODO:  _drop(st);
            }
        }
        SPDLOG_INFO("Indexer thread joined");
    }

    MutableBTreePtr Indexer::_build(std::stop_token st, const Key& key, const IndexParams& idx)
    {
        constexpr int CHECK_PERIOD = 100;

        SPDLOG_DEBUG_MODULE(LOG_GC, "Build index: {}:{} - {}", key.first, key.second, idx._ddl.dump());

        auto db_id = key.first;
        auto tid = idx._ddl["table_id"];
        auto index_id = idx._ddl["id"];

        assert(idx._ddl["action"] == "create_index");

        // index column positions
        std::vector<uint32_t> idx_cols;
        for (auto const& col: idx._ddl["columns"]) {
            idx_cols.push_back(col["position"]);
        }
        auto table = TableMgr::get_instance()->get_table(db_id, tid, idx._xid);

        // create an index root
        MutableBTreePtr root = table->create_index_root(index_id, idx_cols);
        auto key_fields = table->extent_schema()->get_fields(table->get_column_names(idx_cols));

        // additional fields in the root schema to keep extent and row ids
        auto value_fields = std::make_shared<FieldArray>(2);
        uint32_t row_id = 0;
        uint64_t saved_extent_id = 0;

        for (auto row_i = table->begin(); row_i != table->end(); ++row_i) {
            if(st.stop_requested()) {
                root->truncate();
                return {};
            }
            if (row_id % CHECK_PERIOD == 0 && !_check_work_state(key)) {
                return root;
            }
            auto extent_id = row_i.extent_id();
            if (saved_extent_id != extent_id) {
                row_id = 0;
                saved_extent_id = extent_id;
            }

            (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);
            (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(row_id);

            // insert key
            auto &&svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, *row_i);
            root->insert(svalue);

            ++row_id;
        }
        return root;
    }

    bool Indexer::_check_work_state(const Key& key) {
        std::unique_lock g(_m);
        auto const& params = _work_set[key];
        // index drop requested while we've been building it
        if (params._ddl.is_null()) {
            return false;
        }
        return true;
    }

    void Indexer::_commit_build(MutableBTreePtr root, const Key& key, const IndexParams& idx) 
    {
        if (!root) {
            //The build was cancelled as due to a shutdown.
            //When the system restarts it will check the redis precommit hash
            //and restart the build process.
            return;
        }

        auto db_id = key.first;
        auto tid = idx._ddl["table_id"];
        XidLsn xid{idx._xid};

        IndexParams work_item;

        bool redis_commit = false;
        {
            std::unique_lock g(_m);

            // fetch the latest state of the work item before we erase it
            work_item = _work_set[key];
            _work_set.erase(key);

            // if all work items with the give xid are completed, commit redis
            auto it = std::ranges::find_if(_work_set,
                    [&](auto const& v) {
                        return v.first.first == db_id && v.second._xid == idx._xid;
                    });
            if (it == _work_set.end()) {
                redis_commit = true;
            }

            //TODO: remove the assert and handle the case when the index was deleted while building
            // (use set_index_sate(...sys_tbl::IndexNames::State::DELETED)).
            // This case should never happen in the synchronized version
            assert(!work_item._ddl.is_null());

            auto extent_id = root->finalize();
            auto&& meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, tid, idx._xid);
            meta.roots.emplace_back(key.second, extent_id);
            sys_tbl_mgr::Client::get_instance()->update_roots(db_id, tid, idx._xid, meta);

            _cv_done.notify_one();
        }

        // notify redis tables
        if (redis_commit) {
            _redis_ddl.commit_index_ddl(key.first, idx._xid);
        } else {
            SPDLOG_INFO("Pending items found {}, {}, {}", key.first, key.second, idx._xid);
            //TODO: not sure if it's expected?
            assert(false);
        }

        if (work_item._ddl.is_null()) {
            // we got drop index while building it, use the last XID
            _redis_ddl.commit_index_ddl(key.first, work_item._xid);
        }
    }
}
