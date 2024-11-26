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

    void Indexer::drop(uint64_t db_id, uint64_t index_id, uint64_t xid, uint64_t completed_xid)
    {
        std::scoped_lock g(_m);
        Key key(db_id, index_id);
        auto it = _work_set.find(key);
        if (it == _work_set.end()) {
            // note: the work item has _ddl.empty() == true
            // it means to drop the index
            _work_set[key] = {xid, completed_xid, {}};
            _queue.push(key);
            _cv.notify_one();
        } else {
            // clear DDL, it will tell the worker to cancel the index build
            it->second._ddl = {};
        }
    }

    void Indexer::wait_for_completion(uint64_t db_id, uint64_t tid)
    {
        std::unique_lock g(_m);
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
                if (!_commit_build(key, params)) {
                    root->truncate();
                }
                root->finalize();
                //TODO: update the roots table
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
                break;
            }
            if (row_id % CHECK_PERIOD == 0 && !_check_work_state(key)) {
                break;
            }
            auto extent_id = row_i.extent_id();
            if (saved_extent_id != extent_id) {
                row_id = 0;
                saved_extent_id = extent_id;
            }

            (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);
            (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(row_id);

            auto &&svalue = std::make_shared<KeyValueTuple>(key_fields, value_fields, *row_i);
            root->insert(svalue);

            ++row_id;
        }
        root->finalize();
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

    bool Indexer::_commit_build(const Key& key, const IndexParams& idx) 
    {
        IndexParams work_item;

        bool redis_commit = false;
        {
            std::unique_lock g(_m);

            // fetch the latest state of the work item before we erase it
            work_item = _work_set[key];
            _work_set.erase(key);

            auto it = std::ranges::find_if(_work_set,
                    [&](auto const& v) {
                        return v.first.first == key.first && v.second._xid == idx._xid;
                    });
            if (it == _work_set.end()) {
                redis_commit = true;
            }

            _cv_done.notify_one();
        }

        if (redis_commit) {
            _redis_ddl.commit_index_ddl(key.first, idx._xid);
        } else {
            //TODO: not sure if it's expected?
            SPDLOG_INFO("Pending items found {}, {}, {}", key.first, key.second, idx._xid);
        }

        if (work_item._ddl.is_null()) {
            // we got drop index while building it, use the last XID
            _redis_ddl.commit_index_ddl(key.first, work_item._xid);
            return false;
        }

        return true;
    }
}
