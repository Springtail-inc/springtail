#include <fmt/core.h>
#include <cassert>

#include <common/common.hh>
#include <common/threaded_test.hh>
#include <common/tracking_allocator.hh>

#include <gtest/gtest.h>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_index_node.hh>
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
        WriteCacheIndexTest() : _ts(std::make_shared<WriteCacheTableSet>(1)) {};
        void SetUp() override;
        std::vector<WriteCacheIndexTestRequestPtr> get_requests() override;
        void verify() override;
        void TearDown() override;

    private:
        WriteCacheTableSetPtr _ts; // init with 1 partition so everything collides
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

    /** helper to compare plain object vectors */
    template<class T>
    static void vec_eq(const std::vector<T>& lhs, const std::vector<T>& rhs)
    {
        ASSERT_EQ(lhs.size(), rhs.size());
        auto [i1, i2] = std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
        ASSERT_TRUE(i1 == lhs.end() && i2 == rhs.end());
    }

    template<class T>
    static void vec_eq(const std::vector<T>& lhs, const std::vector<T>& rhs, bool (*cmp_fn)(const T&, const T&))
    {
        ASSERT_EQ(lhs.size(), rhs.size());
        auto [i1, i2] = std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), cmp_fn);
        ASSERT_TRUE(i1 == lhs.end() && i2 == rhs.end());
    }

    /** comparator for write cache index row vectors */
    static bool row_cmp_fn(const WriteCacheIndexRowPtr &lhs, const WriteCacheIndexRowPtr &rhs)
    {
        return (lhs->eid == rhs->eid && lhs->xid == rhs->xid &&
                lhs->xid_seq == rhs->xid_seq && lhs->pkey == rhs->pkey);
    }

    /** comparator for table change vectors */
    static bool table_change_cmp_fn(const WriteCacheIndexTableChangePtr &lhs, const WriteCacheIndexTableChangePtr &rhs)
    {
        return (lhs->tid == rhs->tid && lhs->xid == rhs->xid && lhs->xid_seq == rhs->xid_seq);
    }

    void
    WriteCacheIndexTestRequest::_process_request()
    {
        switch(_type) {
            case Type::ADD_ROW:
                std::cout << "ADD ROW request\n";
                _ts->add_rows(_tid, _eid, _data);
                break;
            case Type::EVICT_TABLE:
                std::cout << "EVICT TABLE request\n";
                _ts->evict_table(_tid, _start_xid, _end_xid);
                break;
            case Type::ADD_TABLE_CHANGE: {
                std::cout << "ADD TABLE CHANGE request\n";
                WriteCacheIndexTableChangePtr req = std::make_shared<WriteCacheIndexTableChange>(_tid, _start_xid, _xid_seq, WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE);
                _ts->add_table_change(req);
                break;
            }
            case Type::EVICT_TABLE_CHANGE:
                std::cout << "EVICT TABLE CHANGE request\n";
                _ts->evict_table_changes(_tid, _start_xid, _end_xid);
                break;
        }
    }

    void
    WriteCacheIndexTest::SetUp()
    {
        springtail::springtail_init();
        std::cout << "Init allocated bytes=" << TrackingAllocatorStats<TrackingAllocatorTags::TAG_WRITE_CACHE>::get_instance()->get_allocated_bytes() << std::endl;
        _ts->dump();
    }

    void
    WriteCacheIndexTest::TearDown()
    {
        std::cout << "Shutdown allocated bytes=" << TrackingAllocatorStats<TrackingAllocatorTags::TAG_WRITE_CACHE>::get_instance()->get_allocated_bytes() << std::endl;
        _ts->dump();
    }

    void
    WriteCacheIndexTest::_make_rows(uint64_t eid, uint64_t xid,
                                    uint64_t xid_seq, int rid_start, int count,
                                    std::vector<WriteCacheIndexRowPtr> &rows)
    {
        for (int i = 0; i < count; i++) {
            // pkey, xid,
            rows.push_back(std::make_shared<WriteCacheIndexRow>("data", fmt::format("{}", rid_start+i), eid, xid, xid_seq, WriteCacheIndexRow::RowOp::INSERT));
        }
    }

    WriteCacheIndexTestRequestPtr
    WriteCacheIndexTest::_make_row_request(uint64_t tid, uint64_t eid, uint64_t xid,
                                           uint64_t xid_seq, int rid_start, int count)
    {
        std::vector<WriteCacheIndexRowPtr> rows;
        _make_rows(eid, xid, xid_seq, rid_start, count, rows);
        return std::make_shared<WriteCacheIndexTestRequest>(_ts, tid, eid, rows);
    }

    WriteCacheIndexTestRequestPtr
    WriteCacheIndexTest::_make_eviction_request(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        return std::make_shared<WriteCacheIndexTestRequest>(_ts, start_xid, end_xid, tid,
            WriteCacheIndexTestRequest::Type::EVICT_TABLE);
    }

    WriteCacheIndexTestRequestPtr
    WriteCacheIndexTest::_make_table_change_request(uint64_t tid, uint64_t xid, uint64_t xid_seq)
    {
        return std::make_shared<WriteCacheIndexTestRequest>(_ts, xid, xid_seq, tid);
    }

    WriteCacheIndexTestRequestPtr
    WriteCacheIndexTest::_make_table_change_eviction_request(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        return std::make_shared<WriteCacheIndexTestRequest>(_ts, start_xid, end_xid, tid,
            WriteCacheIndexTestRequest::Type::EVICT_TABLE_CHANGE);
    }

    std::vector<WriteCacheIndexTestRequestPtr>
    WriteCacheIndexTest::get_requests()
    {
        std::vector<WriteCacheIndexTestRequestPtr> requests;

        std::cout << "\n*** Starting test phase: " << _phase << std::endl;

        switch(_phase) {
            case 1: {
                // tid=1, eid=1, xid=1, rid=1,2
                requests.push_back(_make_row_request(1, 1, 1, 1, 1, 2));

                // tid=1, eid=1, xid=1, rid=5,6
                requests.push_back(_make_row_request(1, 1, 1, 1, 5, 2));

                // tid=1, eid=2, xid=1, rid=1,2
                requests.push_back(_make_row_request(1, 2, 1, 1, 1, 2));

                // tid=1, eid=2, xid=1, rid=5,6
                requests.push_back(_make_row_request(1, 2, 1, 1, 5, 2));

                // tid=2, eid=1, xid=1, rid=1,2
                requests.push_back(_make_row_request(2, 1, 1, 1, 1, 2));

                // tid=2, eid=1, xid=1, rid=5,6
                requests.push_back(_make_row_request(2, 1, 1, 1, 5, 2));

                // tid=2, eid=2, xid=1, rid=1,2
                requests.push_back(_make_row_request(2, 2, 1, 1, 1, 2));

                // tid=2, eid=2, xid=1, rid=5,6
                requests.push_back(_make_row_request(2, 2, 1, 1, 5, 2));

                // tid=1, eid=1, xid=2, rid=1,2
                requests.push_back(_make_row_request(1, 1, 2, 1, 1, 2));

                // tid=1, eid=1, xid=2, rid=5,6
                requests.push_back(_make_row_request(1, 1, 2, 1, 5, 2));

                // tid=2, eid=1, xid=2, rid=1,2
                requests.push_back(_make_row_request(2, 1, 2, 1, 1, 2)); //

                // tid=1, eid=2, xid=3, rid=5,6
                requests.push_back(_make_row_request(1, 2, 3, 1, 5, 2));

                // tid=2, eid=1, xid=3, rid=1,2
                requests.push_back(_make_row_request(2, 1, 3, 1, 1, 2)); //

                // tid=2, eid=1, xid=3, rid=5,6
                requests.push_back(_make_row_request(2, 1, 3, 1, 5, 2)); //

                // tid=2, eid=2, xid=4, rid=1,2
                requests.push_back(_make_row_request(2, 2, 4, 1, 1, 2));

                // tid=2, eid=3, xid=4, rid=5,6
                requests.push_back(_make_row_request(2, 3, 4, 1, 5, 2));

                // tid=2, eid=1, xid=4, rid=7,8
                requests.push_back(_make_row_request(2, 1, 4, 1, 7, 2));

                // tid=2, eid=4, xid=3, rid=1
                requests.push_back(_make_row_request(2, 4, 3, 1, 1, 1));
                // tid=2, eid=4, xid=4, rid=1
                requests.push_back(_make_row_request(2, 4, 4, 1, 1, 1));
                // tid=2, eid=5, xid=3, rid=1
                requests.push_back(_make_row_request(2, 5, 3, 1, 1, 1));

                return requests;
            }

            case 2: {
                // evict tid=2, xid=(1:3]
                requests.push_back(_make_eviction_request(2, 1, 3));

                // add row tid=2, eid=1, xid=5, rid=1,2
                requests.push_back(_make_row_request(2, 1, 5, 1, 1, 2));

                // add row tid=2, eid=1, xid=5, rid=1 (xid_seq=2)
                requests.push_back(_make_row_request(2, 1, 5, 2, 1, 1));

                // add row tid=1, eid=1, xid=6, rid=1,2
                requests.push_back(_make_row_request(1, 1, 6, 1, 1, 2));

                // add row tid=1, eid=2, xid=6, rid=5,6
                requests.push_back(_make_row_request(1, 2, 6, 1, 5, 2));

                return requests;
            }

            case 3: {
                // evict tid=1, xid=(1:3]
                requests.push_back(_make_eviction_request(1, 0, 3));
                // evict tid=2, xid=(3:5]
                requests.push_back(_make_eviction_request(2, 3, 5));

                return requests;
            }

            case 4: {
                // shutdown, clear everything
                // evict tid=1, xid=(0:6]
                requests.push_back(_make_eviction_request(1, 0, 6));
                // evict tid=2, xid=(0:6]
                requests.push_back(_make_eviction_request(2, 0, 6));

                return requests;
            }

            case 5: {
                // add table change: tid, xid, xid_seq
                requests.push_back(_make_table_change_request(1, 1, 0));
                requests.push_back(_make_table_change_request(1, 1, 1));
                requests.push_back(_make_table_change_request(1, 2, 0));
                requests.push_back(_make_table_change_request(1, 2, 1));
                requests.push_back(_make_table_change_request(1, 3, 0));
                requests.push_back(_make_table_change_request(1, 3, 1));
                requests.push_back(_make_table_change_request(2, 1, 0));
                requests.push_back(_make_table_change_request(2, 1, 1));
                requests.push_back(_make_table_change_request(2, 2, 0));
                return requests;
            }

            case 6: {
                // evict table changes for tid=1, xids=1:4
                requests.push_back(_make_table_change_eviction_request(1,1,4));
                // add table changes: tid=3, xid, xid_seq
                requests.push_back(_make_table_change_request(3, 2, 1));
                requests.push_back(_make_table_change_request(3, 3, 1));
                requests.push_back(_make_table_change_request(3, 3, 1));
                // evict table changes for tid=2, xids=1:4
                requests.push_back(_make_table_change_eviction_request(2,0,4));
                return requests;
            }

            default:
                std::cout << "Done with testing\n";
                return requests; // return empty requests
        }
    }

    void
    WriteCacheIndexTest::verify()
    {
        std::cout << "\n***Verifying test phase: " << _phase << std::endl;
        uint64_t cursor=0;

        switch(_phase) {
            case 1: {
                std::cout << "Checking table IDs\n";
                std::vector<int64_t> tids;
                uint64_t end_offset;
                cursor = 0;
                int res = _ts->get_tids(0, 4, 10, cursor, end_offset, tids); // xid 0:4, count=10
                ASSERT_EQ(res, 2);
                ASSERT_NO_FATAL_FAILURE(vec_eq(tids, {1,2}));

                std::cout << "\nChecking extent IDs, count=10\n";
                std::vector<int64_t> eids;
                cursor = 0;
                res = _ts->get_eids(2, 2, 4, 10, cursor, eids); // tid=2, xid 2:4
                ASSERT_EQ(res, 5);
                ASSERT_EQ(cursor, 7);
                ASSERT_NO_FATAL_FAILURE(vec_eq(eids, {1,2,3,4,5}));

                // Even though we get back eids: 1,2,3,4,5, these are ordered
                // and deduplicated.  The actual data for TID=2, looks like:
                // XID:3 -> EIDS: {1,4,5}; XID:4 -> EIDS: {1,2,3,4}

                std::cout << "\nChecking extent IDS, count=2\n";
                cursor = 0;
                eids.clear();
                res = _ts->get_eids(2, 2, 4, 2, cursor, eids); // tid=2, xid 2:4, count=2
                ASSERT_EQ(res, 2);
                ASSERT_EQ(cursor, 2);
                // This is returning the first two eids from XID 3 which are 1,4
                ASSERT_NO_FATAL_FAILURE(vec_eq(eids, {1,4}));

                // don't reset cursor
                std::cout << "\nChecking extent IDs, cursor=" << cursor << " count=3\n";
                eids.clear();
                res = _ts->get_eids(2, 2, 4, 3, cursor, eids);
                ASSERT_EQ(res, 3);
                ASSERT_EQ(cursor, 5);
                // Cursor was at 2, so we should now get 5, 1, 2
                ASSERT_NO_FATAL_FAILURE(vec_eq(eids, {1,2,5}));

                std::cout << "\nChecking extent IDs, cursor=" << cursor << " count=3\n";
                eids.clear();
                res = _ts->get_eids(2, 2, 4, 3, cursor, eids);
                ASSERT_EQ(res, 2);
                ASSERT_EQ(cursor, 7);
                // Cursor is at 5, so we should now get 3,4
                ASSERT_NO_FATAL_FAILURE(vec_eq(eids, {3,4}));

                std::cout << "\nChecking extent IDs, cursor=0, count=3\n";
                cursor = 0;
                eids.clear();
                res = _ts->get_eids(2, 2, 4, 3, cursor, eids);
                ASSERT_EQ(cursor, 3);

                std::cout << "\nChecking extent IDs, cursor=" << cursor << ", count=3\n";
                eids.clear();
                res = _ts->get_eids(2, 2, 4, 3, cursor, eids);
                ASSERT_EQ(res, 3);
                // Cursor is at 3, so we should now get 1,2,3
                ASSERT_NO_FATAL_FAILURE(vec_eq(eids, {1,2,3}));
                ASSERT_EQ(cursor, 6);

                // set clean flag for tid=2, eid=1 and 4, between xids 2-4
                std::cout << "\nChecking extent IDs, cursor=2, after clean flag\n";
                _ts->set_clean_flag(2, 1, 2, 4);
                _ts->set_clean_flag(2, 4, 2, 4);
                cursor = 2;
                eids.clear();
                res = _ts->get_eids(2, 2, 4, 3, cursor, eids);
                ASSERT_EQ(res, 3);
                ASSERT_EQ(cursor, 6);
                // Cursor was at 2, so we should now get 5, 2, 3 skipping 1
                ASSERT_NO_FATAL_FAILURE(vec_eq(eids, {2,3,5}));

                std::cout << "Checking rows\n";
                std::vector<WriteCacheIndexRowPtr> rows_result;
                cursor = 0;
                res = _ts->get_rows(2, 1, 1, 3, 10, cursor, rows_result); // tid=2, eid=1, xid 1:3
                ASSERT_EQ(res, 6);

                std::vector<WriteCacheIndexRowPtr> rows_expected;
                _make_rows(1, 2, 1, 1, 2, rows_expected);
                _make_rows(1, 3, 1, 1, 2, rows_expected);
                _make_rows(1, 3, 1, 5, 2, rows_expected);
                ASSERT_NO_FATAL_FAILURE(vec_eq(rows_expected, rows_result, row_cmp_fn));

                std::cout << "Phase 1 allocated bytes=" << TrackingAllocatorStats<TrackingAllocatorTags::TAG_WRITE_CACHE>::get_instance()->get_allocated_bytes() << std::endl;

                break;
            }

            case 2: {
                std::cout << "Checking extent IDs\n";
                std::vector<int64_t> eids;
                cursor = 0;
                int res = _ts->get_eids(2, 1, 3, 10, cursor, eids); // tid=2, xid 1:3
                ASSERT_EQ(res, 0);

                std::cout << "Checking rows\n";
                std::vector<WriteCacheIndexRowPtr> rows_result;
                cursor = 0;
                res = _ts->get_rows(2, 1, 2, 5, 10, cursor, rows_result); // tid=2, eid=1, xid 2:5
                ASSERT_EQ(res, 5);
                ASSERT_EQ(cursor, 5);

                std::vector<WriteCacheIndexRowPtr> rows_expected;
                _make_rows(1, 4, 1, 7, 2, rows_expected);
                _make_rows(1, 5, 1, 1, 2, rows_expected);
                _make_rows(1, 5, 2, 1, 1, rows_expected);
                ASSERT_NO_FATAL_FAILURE(vec_eq(rows_expected, rows_result, row_cmp_fn));

                std::cout << "Checking cursor for get rows\n";
                cursor = 3;
                rows_result.clear();
                res = _ts->get_rows(2, 1, 2, 5, 10, cursor, rows_result); // tid=2, eid=1, xid 2:5
                ASSERT_EQ(res, 2);
                rows_expected.clear();
                _make_rows(1, 5, 1, 2, 1, rows_expected);
                _make_rows(1, 5, 2, 1, 1, rows_expected);
                ASSERT_NO_FATAL_FAILURE(vec_eq(rows_expected, rows_result, row_cmp_fn));

                std::cout << "Checking rows\n";
                rows_result.clear();
                cursor = 0;
                res = _ts->get_rows(1, 2, 2, 6, 10, cursor, rows_result); // tid=1, eid=2, xid 2:6
                ASSERT_EQ(res, 4);

                rows_expected.clear();
                _make_rows(2, 3, 1, 5, 2, rows_expected);
                _make_rows(2, 6, 1, 5, 2, rows_expected);
                ASSERT_NO_FATAL_FAILURE(vec_eq(rows_expected, rows_result, row_cmp_fn));

                break;
            }

            case 3: {
                std::cout << "Checking extent IDs\n";
                std::vector<int64_t> eids;
                cursor = 0;
                int res = _ts->get_eids(1, 0, 5, 10, cursor, eids); // tid=1, xid 0:5
                ASSERT_EQ(res, 0);

                std::cout << "Checking rows\n";
                std::vector<WriteCacheIndexRowPtr> rows_result;
                cursor = 0;
                res = _ts->get_rows(2, 1, 0, 5, 10, cursor, rows_result); // tid=2, eid=1, xid 2:5
                ASSERT_EQ(res, 4);

                std::vector<WriteCacheIndexRowPtr> rows_expected;
                _make_rows(1, 1, 1, 1, 2, rows_expected);
                _make_rows(1, 1, 1, 5, 2, rows_expected);
                ASSERT_NO_FATAL_FAILURE(vec_eq(rows_expected, rows_result, row_cmp_fn));

                std::cout << "Checking rows\n";
                rows_result.clear();
                cursor = 0;
                res = _ts->get_rows(1, 2, 2, 6, 10, cursor, rows_result); // tid=1, eid=2, xid 2:6
                ASSERT_EQ(res, 2);

                rows_expected.clear();
                _make_rows(2, 6, 1, 5, 2, rows_expected);
                ASSERT_NO_FATAL_FAILURE(vec_eq(rows_expected, rows_result, row_cmp_fn));

                break;
            }

            case 4: {
                std::cout << "Checking extent IDs\n";
                std::vector<int64_t> eids;
                cursor = 0;
                int res = _ts->get_eids(1, 0, 6, 10, cursor, eids); // tid=1, xid (0:6]
                ASSERT_EQ(res, 0);
                cursor = 0;
                res = _ts->get_eids(2, 0, 6, 10, cursor, eids); // tid=2, xid (0:6]
                ASSERT_EQ(res, 0);

                std::cout << "Checking rows\n";
                std::vector<WriteCacheIndexRowPtr> rows_result;
                res = _ts->get_rows(2, 1, 0, 6, 10, cursor, rows_result); // tid=2, eid=1, xid 2:5
                ASSERT_EQ(res, 0);

                break;
            }

            case 5: {
                std::cout << "Checking table changes\n";
                std::vector<WriteCacheIndexTableChangePtr> changes;
                _ts->get_table_changes(1, 1, 5, changes);
                ASSERT_EQ(changes.size(), 4);

                std::vector<WriteCacheIndexTableChangePtr> expected;
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 2, 0,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 2, 1,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 3, 0,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 3, 1,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                ASSERT_NO_FATAL_FAILURE(vec_eq(changes, expected, table_change_cmp_fn));
                break;
            }

            case 6: {
                std::cout << "Checking table changes\n";
                std::vector<WriteCacheIndexTableChangePtr> changes;
                _ts->get_table_changes(1, 0, 5, changes);
                ASSERT_EQ(changes.size(), 2);

                std::vector<WriteCacheIndexTableChangePtr> expected;
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 1, 0,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 1, 1,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                ASSERT_NO_FATAL_FAILURE(vec_eq(changes, expected, table_change_cmp_fn));

                changes.clear();
                _ts->get_table_changes(2, 0, 5, changes);
                ASSERT_EQ(changes.size(), 0);

                changes.clear();
                _ts->get_table_changes(3, 0, 5, changes);
                ASSERT_EQ(changes.size(), 3);

                break;

            }
        }
        std::cout << "Verified test phase: " << _phase << " successfully\n";

        // move on to next phase
        _phase++;

        return;
    }

    TEST_F(WriteCacheIndexTest, Threaded_Test)
    {
        // Create the thread tester
        ThreadedTest<WriteCacheIndexTest, WriteCacheIndexTestRequest> tester(4);
        tester.start();
    }
} // namespace springtail

/** Deprecated */
/*
static int s_id=0;

void
add_row(springtail::WriteCacheTableSet &ts, uint64_t tid, uint64_t eid, uint64_t xid, int rid=s_id)
{
    springtail::WriteCacheIndexRowPtr row =
        std::make_shared<springtail::WriteCacheIndexRow>("data", fmt::format("key:{}", rid), eid, xid, 0, springtail::WriteCacheIndexRow::RowOp::INSERT);

    std::cout << fmt::format("\nInserting row: tid: {}, eid: {}, rid: {}, xid: {}, xid_seq: {}, key:{}\n",
                             tid, eid, rid, xid, s_id, rid);

    if (rid == s_id) {
        s_id++;
    }

    std::vector<springtail::WriteCacheIndexRowPtr> rows;
    rows.push_back(row);
    ts.add_rows(tid, eid, rows);
}

std::string
dump_row_data(springtail::WriteCacheIndexRowPtr row)
{
    return fmt::format("XID: {}:{} RID: {}", row->xid, row->xid_seq, row->pkey);
}

void
dump_rows(const std::vector<springtail::WriteCacheIndexRowPtr> rows)
{
    std::cout << "Dumping row data\n";
    for (auto r: rows) {
        std::cout << dump_row_data(r) << std::endl;
    }
}

void manual_test()
{
    springtail::WriteCacheTableSet ts;

    add_row(ts, 1, 1, 5); // tid, eid, xid, rid=s_id
    add_row(ts, 1, 1, 2);
    add_row(ts, 1, 1, 9);
    add_row(ts, 1, 1, 7);
    add_row(ts, 1, 1, 10);

    ts.dump();

    std::vector<springtail::WriteCacheIndexRowPtr> rows;
    uint64_t cursor = 0;
    ts.get_rows(1, 1, 6, 9, 10, cursor, rows);
    dump_rows(rows);
    rows = {};

    add_row(ts, 1, 2, 8);
    add_row(ts, 1, 2, 7);
    add_row(ts, 1, 2, 15);

    ts.dump();

    cursor = 0;
    ts.get_rows(1, 1, 6, 9, 10, cursor, rows);
    dump_rows(rows);
    rows = {};

    // update key:2 with xid 10
    add_row(ts, 1, 1, 10, 2);

    ts.dump();

    std::cout << "Allocated bytes: " << springtail::TrackingAllocatorStats<springtail::TrackingAllocatorTags::TAG_WRITE_CACHE>::get_instance()->get_allocated_bytes() << std::endl;

    std::cout << "Evicting extent: tid=1, eid=2, xids: 8:15\n";
    ts.evict_table(1, 8, 15);

    ts.dump();

    std::cout << "Evicting extent: tid=1, eid=1, xids: 5:7\n";
    ts.evict_table(1, 5, 7);

    ts.dump();

    std::cout << "Allocated bytes: " << springtail::TrackingAllocatorStats<springtail::TrackingAllocatorTags::TAG_WRITE_CACHE>::get_instance()->get_allocated_bytes() << std::endl;
}
*/

