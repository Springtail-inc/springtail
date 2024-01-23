#include <fmt/core.h>
#include <cassert>

#include <common/common.hh>
#include <common/threaded_test.hh>
#include <common/tracking_allocator.hh>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_index_node.hh>
#include <write_cache/write_cache_table_set.hh>
#include <write_cache/write_cache_index_test.hh>

namespace springtail {

    /** helper to compare plain object vectors */
    template<class T>
    static bool vec_eq(const std::vector<T>& lhs, const std::vector<T>& rhs)
    {
        auto [i1, i2] = std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
        return (i1 == lhs.end() && i2 == rhs.end());
    }

    template<class T>
    static bool vec_eq(const std::vector<T>& lhs, const std::vector<T>& rhs, bool (*cmp_fn)(const T&, const T&))
    {
        auto [i1, i2] = std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), cmp_fn);
        return (i1 == lhs.end() && i2 == rhs.end());
    }

    /** comparator for write cache index row vectors */
    static bool row_cmp_fn(const WriteCacheIndexRowPtr &lhs, const WriteCacheIndexRowPtr &rhs)
    {
        return (lhs->eid == rhs->eid && lhs->xid == rhs->xid &&
                lhs->xid_seq == rhs->xid_seq && lhs->pkey == rhs->pkey);
    }

    static bool table_change_cmp_fn(const WriteCacheIndexTableChangePtr &lhs, const WriteCacheIndexTableChangePtr &rhs)
    {
        return (lhs->tid == rhs->tid && lhs->xid == rhs->xid && lhs->xid_seq == rhs->xid_seq);
    }

    /** helper to compare write cache index row vectors using comparator */
    static bool row_cmp(const std::vector<WriteCacheIndexRowPtr> &lhs, const std::vector<WriteCacheIndexRowPtr> &rhs)
    {
        auto [i1, i2] = std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), row_cmp_fn);
        if (!(i1 == lhs.end() && i2 == rhs.end())) {
            std::cout << "Mismatch found: " << (*i1)->dump() << " cmp " << (*i2)->dump() << std::endl;
        }
        return (i1 == lhs.end() && i2 == rhs.end());
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
    WriteCacheIndexTest::init()
    {
        std::cout << "Init allocated bytes=" << TrackingAllocatorStats<TrackingAllocatorTags::TAG_WRITE_CACHE>::get_instance()->get_allocated_bytes() << std::endl;
        _ts->dump();
    }

    void
    WriteCacheIndexTest::shutdown()
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

    bool
    WriteCacheIndexTest::verify()
    {
        std::cout << "\n***Verifying test phase: " << _phase << std::endl;
        uint64_t cursor=0;

        switch(_phase) {
            case 1: {
                std::cout << "Checking table IDs\n";
                std::vector<int64_t> tids;
                cursor = 0;
                int res = _ts->get_tids(0, 4, 10, cursor, tids); // xid 0:4, count=10
                assert(res == 2);
                assert(vec_eq(tids, {1,2}));

                std::cout << "Checking extent IDs\n";
                std::vector<int64_t> eids;
                cursor = 0;
                res = _ts->get_eids(2, 2, 4, 10, cursor, eids); // tid=2, xid 2:4
                assert(res == 3);
                assert(cursor == 3);
                assert(vec_eq(eids, {1,2,3}));

                cursor = 2;
                eids.clear();
                res = _ts->get_eids(2, 2, 4, 10, cursor, eids);
                assert(cursor == 3);
                assert(res = 1);

                std::cout << "Checking rows\n";
                std::vector<WriteCacheIndexRowPtr> rows_result;
                cursor = 0;
                res = _ts->get_rows(2, 1, 1, 3, 10, cursor, rows_result); // tid=2, eid=1, xid 1:3
                assert(res == 6);

                std::vector<WriteCacheIndexRowPtr> rows_expected;
                _make_rows(1, 2, 1, 1, 2, rows_expected);
                _make_rows(1, 3, 1, 1, 2, rows_expected);
                _make_rows(1, 3, 1, 5, 2, rows_expected);
                assert(row_cmp(rows_expected, rows_result));

                std::cout << "Phase 1 allocated bytes=" << TrackingAllocatorStats<TrackingAllocatorTags::TAG_WRITE_CACHE>::get_instance()->get_allocated_bytes() << std::endl;

                break;
            }

            case 2: {
                std::cout << "Checking extent IDs\n";
                std::vector<int64_t> eids;
                cursor = 0;
                int res = _ts->get_eids(2, 1, 3, 10, cursor, eids); // tid=2, xid 1:3
                assert(res == 0);

                std::cout << "Checking rows\n";
                std::vector<WriteCacheIndexRowPtr> rows_result;
                cursor = 0;
                res = _ts->get_rows(2, 1, 2, 5, 10, cursor, rows_result); // tid=2, eid=1, xid 2:5
                assert(res == 5);
                assert(cursor == 5);

                std::vector<WriteCacheIndexRowPtr> rows_expected;
                _make_rows(1, 4, 1, 7, 2, rows_expected);
                _make_rows(1, 5, 1, 1, 2, rows_expected);
                _make_rows(1, 5, 2, 1, 1, rows_expected);
                assert(row_cmp(rows_expected, rows_result));

                std::cout << "Checking cursor for get rows\n";
                cursor = 3;
                rows_result.clear();
                res = _ts->get_rows(2, 1, 2, 5, 10, cursor, rows_result); // tid=2, eid=1, xid 2:5
                assert(res == 2);
                rows_expected.clear();
                _make_rows(1, 5, 1, 2, 1, rows_expected);
                _make_rows(1, 5, 2, 1, 1, rows_expected);
                assert(row_cmp(rows_expected, rows_result));

                std::cout << "Checking rows\n";
                rows_result.clear();
                cursor = 0;
                res = _ts->get_rows(1, 2, 2, 6, 10, cursor, rows_result); // tid=1, eid=2, xid 2:6
                assert(res == 4);

                rows_expected.clear();
                _make_rows(2, 3, 1, 5, 2, rows_expected);
                _make_rows(2, 6, 1, 5, 2, rows_expected);
                assert(row_cmp(rows_expected, rows_result));

                break;
            }

            case 3: {
                std::cout << "Checking extent IDs\n";
                std::vector<int64_t> eids;
                cursor = 0;
                int res = _ts->get_eids(1, 0, 5, 10, cursor, eids); // tid=1, xid 0:5
                assert(res == 0);

                std::cout << "Checking rows\n";
                std::vector<WriteCacheIndexRowPtr> rows_result;
                cursor = 0;
                res = _ts->get_rows(2, 1, 0, 5, 10, cursor, rows_result); // tid=2, eid=1, xid 2:5
                assert(res == 4);

                std::vector<WriteCacheIndexRowPtr> rows_expected;
                _make_rows(1, 1, 1, 1, 2, rows_expected);
                _make_rows(1, 1, 1, 5, 2, rows_expected);
                assert(row_cmp(rows_expected, rows_result));

                std::cout << "Checking rows\n";
                rows_result.clear();
                cursor = 0;
                res = _ts->get_rows(1, 2, 2, 6, 10, cursor, rows_result); // tid=1, eid=2, xid 2:6
                assert(res == 2);

                rows_expected.clear();
                _make_rows(2, 6, 1, 5, 2, rows_expected);
                assert(row_cmp(rows_expected, rows_result));

                break;
            }

            case 4: {
                std::cout << "Checking extent IDs\n";
                std::vector<int64_t> eids;
                cursor = 0;
                int res = _ts->get_eids(1, 0, 6, 10, cursor, eids); // tid=1, xid (0:6]
                assert(res == 0);
                cursor = 0;
                res = _ts->get_eids(2, 0, 6, 10, cursor, eids); // tid=2, xid (0:6]
                assert(res == 0);

                std::cout << "Checking rows\n";
                std::vector<WriteCacheIndexRowPtr> rows_result;
                res = _ts->get_rows(2, 1, 0, 6, 10, cursor, rows_result); // tid=2, eid=1, xid 2:5
                assert(res == 0);

                break;
            }

            case 5: {
                std::cout << "Checking table changes\n";
                std::vector<WriteCacheIndexTableChangePtr> changes;
                _ts->get_table_changes(1, 1, 5, changes);
                assert(changes.size() == 4);

                std::vector<WriteCacheIndexTableChangePtr> expected;
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 2, 0,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 2, 1,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 3, 0,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 3, 1,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                assert(vec_eq(changes, expected, table_change_cmp_fn));
                break;
            }

            case 6: {
                std::cout << "Checking table changes\n";
                std::vector<WriteCacheIndexTableChangePtr> changes;
                _ts->get_table_changes(1, 0, 5, changes);
                assert(changes.size() == 2);

                std::vector<WriteCacheIndexTableChangePtr> expected;
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 1, 0,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                expected.push_back(std::make_shared<WriteCacheIndexTableChange>(1, 1, 1,
                    WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE));
                assert(vec_eq(changes, expected, table_change_cmp_fn));

                changes.clear();
                _ts->get_table_changes(2, 0, 5, changes);
                assert(changes.size() == 0);

                changes.clear();
                _ts->get_table_changes(3, 0, 5, changes);
                assert(changes.size() == 3);

                break;

            }
        }
        std::cout << "Verified test phase: " << _phase << " successfully\n";

        // move on to next phase
        _phase++;

        return true;
    }
}

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

int main(void) {
    springtail::springtail_init();

    springtail::ThreadedTest<springtail::WriteCacheIndexTest, springtail::WriteCacheIndexTestRequest> tester(4);

    tester.start();

    return 0;
}