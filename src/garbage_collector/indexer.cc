
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
        }
        SPDLOG_INFO("Indexer thread joined");
    }

    void Indexer::_build(std::stop_token st, const Key& key, const IndexParams& idx)
    {
        //TODO for each row
        {
            // TODO: ... insert the row index
            //
            if(!st.stop_requested()) {
                // TODO:... clear partial index
                // abort DDL
                _redis_ddl.abort_index_ddl(key.first, idx._xid);
                return;
            }
            // .... periodically check if the index was dropped while 
            // we were building it
            {
                std::unique_lock g(_m);
                // index drop requested while we'd been building it
                auto const& params = _work_set[key];
                if (params._ddl.is_null()) {
                    // TODO: remove this index from precommitted DDL for this XID
//                  _redis_ddl.abort_index_ddl(key.first, idx._xid, key.second);
//                    _drop(st, key, params);
                    return;
                }
            }
        }
        _commit(key);
    }

    void Indexer::_commit(const Key& key) 
    {
        uint64_t xid{};

        {
            std::unique_lock g(_m);
            xid = _work_set[key]._xid;

            _work_set.erase(key);

            _cv_done.notify_one();


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
