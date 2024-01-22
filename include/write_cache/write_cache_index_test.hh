#pragma once

#include <memory>
#include <vector>

#include <common/threaded_test.hh>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail {

    /** Write cache test request object: encapsulates an add_row or an evict_table request */
    class WriteCacheIndexTestRequest {
    public:
        enum Type : uint8_t {
            ADD_ROW=0,
            EVICT_TABLE=1,
            ADD_TABLE_CHANGE=2,
            EVICT_TABLE_CHANGE=3
        };

        /** add row constructor */
        WriteCacheIndexTestRequest(WriteCacheTableSetPtr ts, uint64_t tid, uint64_t eid, const std::vector<WriteCacheIndexRowPtr> &data)
            : _ts(ts), _type(Type::ADD_ROW), _tid(tid), _eid(eid), _data(data) {}

        /** evict table / evict table change constructor */
        WriteCacheIndexTestRequest(WriteCacheTableSetPtr ts, uint64_t start_xid, uint64_t end_xid, uint64_t tid, Type type)
            : _ts(ts), _type(type), _start_xid(start_xid), _end_xid(end_xid), _tid(tid) {}

        /** add table change constructor */
        WriteCacheIndexTestRequest(WriteCacheTableSetPtr ts, uint64_t start_xid, uint64_t xid_seq, uint64_t tid)
            : _ts(ts), _type(Type::ADD_TABLE_CHANGE), _start_xid(start_xid), _xid_seq(xid_seq), _tid(tid) {}

       /**
         * @brief Overload () for execution from worker thread.
         *        Main entry from worker thread
         */
        void operator()() {
            _process_request();
        }

    private:
        WriteCacheTableSetPtr _ts;
        Type _type;
        uint64_t _start_xid;
        uint64_t _end_xid;
        uint64_t _xid_seq;
        uint64_t _tid;
        uint64_t _eid;
        std::vector<WriteCacheIndexRowPtr> _data;

        /** Function called from overloaded operator(); used to execute the request */
        void _process_request();
    };
    typedef std::shared_ptr<WriteCacheIndexTestRequest> WriteCacheIndexTestRequestPtr;

    /**
     * @brief Write cache index test object; encapsulates the state used by the ThreadedTest
     * Operates in phases determined by the set of requests retrieved by get_requests().
     * Each batch of requests is processed in parallel on multiple threads and is followed
     * by a call to verify().
     */
    class WriteCacheIndexTest : public ThreadTestState<WriteCacheIndexTestRequest> {
    public:
        WriteCacheIndexTest() : _ts(std::make_shared<WriteCacheTableSet>()) {};
        void init() override;
        std::vector<WriteCacheIndexTestRequestPtr> get_requests() override;
        bool verify() override;
        void shutdown() override;

    private:
        WriteCacheTableSetPtr _ts;
        int _phase = 1;

        /** Helper to construct a test request for adding new rows */
        WriteCacheIndexTestRequestPtr
        _make_row_request(uint64_t tid, uint64_t eid, uint64_t xid,
                          uint64_t xid_seq, int rid_start, int count);

        /** Helper to construct a vector of rows */
        void
        _make_rows(uint64_t eid, uint64_t xid,
                   uint64_t xid_seq, int rid_start, int count,
                   std::vector<WriteCacheIndexRowPtr> &rows);

        /** Helper to create table eviction request */
        WriteCacheIndexTestRequestPtr
        _make_eviction_request(uint64_t tid, uint64_t start_xid, uint64_t end_xid);

        /** Helper to create table change addition request */
        WriteCacheIndexTestRequestPtr
        _make_table_change_request(uint64_t tid, uint64_t xid, uint64_t xid_seq);

        /** Helper to create table change eviction request */
        WriteCacheIndexTestRequestPtr
        _make_table_change_eviction_request(uint64_t tid, uint64_t start_xid, uint64_t end_xid);
    };
}