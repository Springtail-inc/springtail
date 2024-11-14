
#include <mutex>
#include <stop_token>
#include <assert.h>
#include <garbage_collector/indexer.hh>
#include <common/logging.hh>
#include <sys_tbl_mgr/table_mgr.hh>

namespace springtail::gc {

    Indexer::Indexer(uint32_t worker_count) {
        assert(worker_count);
        for (auto i = 0; i != worker_count; ++i) {
            _workers.emplace_back([this](std::stop_token st) { task(st); });
        }
        SPDLOG_INFO("Indexer created: {}", worker_count);
    }

    void Indexer::build(IndexParams idx) {
        std::scoped_lock g(_m);
        Key key(idx._db_id, idx._ddl["id"]);
        assert(_work_set.find(key) == _work_set.end());
        _work_set[key] = std::move(idx);
        // notify workers about new item
        _queue.push(key);
        _cv.notify_one();
    }

    void Indexer::drop(uint64_t db_id, uint64_t index_id) {
        std::scoped_lock g(_m);
        Key key(db_id, index_id);
        auto it = _work_set.find(key);
        if (it == _work_set.end()) {
            _work_set[key] = {};
            // notify workers about new item
            _queue.push(key);
            _cv.notify_one();
        } else {
            // clear params, it will tell the worker to cancel the index build
            it->second = {};
        }
    }

    void Indexer::task(std::stop_token st) {
        while(!st.stop_requested()) {
            Key key;
            std::optional<IndexParams> params;

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

            if (params) {
                SPDLOG_DEBUG_MODULE(LOG_GC, "Build index: {}:{} - {}", key.first, key.second, params->_ddl.dump());
                _build(st, std::move(*params));
            } else {
//TODO:  _drop(st);
            }

            {
                std::unique_lock g(_m);
                _work_set.erase(key);
            }
        }
        SPDLOG_INFO("Indexer thread joined");
    }

    void Indexer::_build(std::stop_token st, IndexParams idx) {
//        auto tid = idx._ddl["table_id"];
 //       auto table = TableMgr::get_instance()->get_mutable_table(idx._db_id, tid, idx._completed_xid, idx._xid, true);
    }
}
