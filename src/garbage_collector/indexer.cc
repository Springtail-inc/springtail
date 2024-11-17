
#include <mutex>
#include <stop_token>
#include <assert.h>
#include <algorithm>
#include <garbage_collector/indexer.hh>
#include <common/logging.hh>
#include <sys_tbl_mgr/table_mgr.hh>

namespace springtail::gc {

    Indexer::Indexer(uint32_t worker_count) 
    {
        assert(worker_count);
        for (auto i = 0; i != worker_count; ++i) {
            _workers.emplace_back([this](std::stop_token st) { task(st); });
        }
        SPDLOG_INFO("Indexer created: {}", worker_count);
    }

    void Indexer::build(std::vector<IndexParams> idxs)
    {
        {
            std::scoped_lock g(_m);
            for (auto const& idx: idxs) {
                Key key(idx._db_id, idx._ddl["id"]);
                assert(_work_set.find(key) == _work_set.end());
                _work_set[key] = idx;
                _queue.push(key);
            }
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
            // this will clear the item ddl that is an indication
            // to the worker thread that the index should be dropped
            _work_set[key] = {db_id, xid, completed_xid };
            // notify workers about new item
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
            auto it = std::find_if(_work_set.begin(), _work_set.end(),
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
                SPDLOG_DEBUG_MODULE(LOG_GC, "Build index: {}:{} - {}", key.first, key.second, params._ddl.dump());
                _build(st, key, params);
            } else {
//TODO:  _drop(st);
            }
            _commit(key);
        }
        SPDLOG_INFO("Indexer thread joined");
    }

    void Indexer::_build(std::stop_token st, const Key& key, const IndexParams& idx)
    {
        auto db_id = key.first;
        auto tid = idx._ddl["table_id"];
        auto table = TableMgr::get_instance()->get_mutable_table(db_id, tid, idx._completed_xid, idx._xid, true);

        //TODO for each row
        {
            
            if(!st.stop_requested()) {
                // TODO:... clear partial index
                // abort DDL
                _redis_ddl.abort_index_ddl(db_id, idx._xid);
                return;
            }

            // TODO: ... insert the row index

            // .... periodically check if the index was dropped while 
            // we were building it
            if (!_check_work_state(key)) {
                return;
            }
        }
    }

    bool Indexer::_check_work_state(const Key& key) {
        std::unique_lock g(_m);
        auto const& params = _work_set[key];
        // index drop requested while we've been building it
        if (params._ddl.is_null()) {
            // TODO:... clear partial index
            return false;
        }
        return true;
    }

    void Indexer::_commit(const Key& key) 
    {
        uint64_t xid{};

        {
            std::unique_lock g(_m);
            xid = _work_set[key]._xid;

            _check_work_state(key);
            _work_set.erase(key);
            _cv_done.notify_one();

            // check if there are pending jobs for the giving xid
            auto it = std::find_if(_work_set.begin(), _work_set.end(),
                    [&](auto const& v) { 
                        return v.first.first == key.first && v.second._xid == xid;
                    });

            if (it != _work_set.end()) {
                return;
            }
        }

        // TODO: uncomment after FDW support is added
        //_redis_ddl.commit_index_ddl(key.first, xid);
    }
}
