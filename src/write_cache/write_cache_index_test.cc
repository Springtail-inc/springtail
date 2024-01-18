#include <fmt/core.h>
#include <cassert>

#include <common/threaded_test.hh>
#include <common/common.hh>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_index_node.hh>
#include <write_cache/write_cache_table_set.hh>
#include <write_cache/write_cache_index_test.hh>

namespace springtail {

    void
    WriteCacheIndexTestRequest::_process_request()
    {
        switch(_type) {
            case Type::ADD_ROW:
                _ts->add_rows(_tid, _eid, _data);
                break;
            case Type::EVICT_TABLE:
                _ts->evict_table(_tid, _start_xid, _end_xid);
                break;
        }
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

    std::vector<WriteCacheIndexTestRequestPtr>
    WriteCacheIndexTest::get_requests()
    {
        std::vector<WriteCacheIndexTestRequestPtr> requests;

        std::cout << "Starting test phase: " << _phase << std::endl;

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

                return requests;
            }

            default:
                std::cout << "Done with testing\n";
                return requests; // return empty requests
        }
    }

    template<class T>
    bool vec_eq(const std::vector<T>& lhs, const std::vector<T>& rhs)
    {
        auto [i1, i2] = std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
        return (i1 == lhs.end() && i2 == rhs.end());
    }

    bool row_cmp_fn(const WriteCacheIndexRowPtr &lhs, const WriteCacheIndexRowPtr &rhs)
    {
        return (lhs->eid < rhs->eid ||
            (lhs->eid == rhs->eid && lhs->xid < rhs->xid) ||
            (lhs->eid == rhs->eid && lhs->xid == rhs->xid &&
             lhs->xid_seq < rhs->xid_seq) ||
            (lhs->eid == rhs->eid && lhs->xid == rhs->xid &&
             lhs->xid_seq == rhs->xid_seq && lhs->pkey < rhs->pkey));
    }

    bool row_cmp(const std::vector<WriteCacheIndexRowPtr> &lhs, const std::vector<WriteCacheIndexRowPtr> &rhs)
    {
        auto [i1, i2] = std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), row_cmp_fn);
        return (i1 == lhs.end() && i2 == rhs.end());
    }

    bool
    WriteCacheIndexTest::verify()
    {
        std::cout << "Verifying test phase: " << _phase << std::endl;

        switch(_phase) {
            case 1: {
                std::cout << "Checking table IDs\n";
                std::vector<int64_t> tids;
                int res = _ts->get_tids(0, 4, 10, tids); // xid 0:4, count=10
                assert(res == 2);
                assert(vec_eq(tids, {1,2}));

                std::cout << "Checking extent IDs\n";
                std::vector<int64_t> eids;
                res = _ts->get_eids(2, 2, 4, 10, eids); // tid=2, xid 2:4
                assert(res == 3);
                assert(vec_eq(eids, {1,2,3}));

                std::cout << "Checking rows\n";
                std::vector<WriteCacheIndexRowPtr> rows_result;
                res = _ts->get_rows(2, 1, 1, 3, 10, rows_result); // tid=2, eid=1, xid 1:3

                for (auto r: rows_result) {
                    std::cout << r->dump() << std::endl;
                }

                assert(res == 6);
                /* Expect:
                2,1,2,1, 1,2
                2,1,3,1, 1,2
                2,1,3,1, 5,6 */
                std::vector<WriteCacheIndexRowPtr> rows_expected;
                _make_rows(1, 2, 1, 1, 2, rows_expected);
                _make_rows(1, 3, 1, 1, 2, rows_expected);
                _make_rows(1, 3, 1, 5, 6, rows_expected);

                assert(row_cmp(rows_expected, rows_result));
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

    ts.get_rows(1, 1, 6, 9, 10, rows);
    dump_rows(rows);
    rows = {};

    add_row(ts, 1, 2, 8);
    add_row(ts, 1, 2, 7);
    add_row(ts, 1, 2, 15);

    ts.dump();

    ts.get_rows(1, 1, 6, 9, 10, rows);
    dump_rows(rows);
    rows = {};

    // update key:2 with xid 10
    add_row(ts, 1, 1, 10, 2);

    ts.dump();

    std::cout << "Allocated bytes: " << springtail::TrackingAllocatorStats::get_instance()->get_allocated_bytes() << std::endl;

    std::cout << "Evicting extent: tid=1, eid=2, xids: 8:15\n";
    ts.evict_table(1, 8, 15);

    ts.dump();

    std::cout << "Evicting extent: tid=1, eid=1, xids: 5:7\n";
    ts.evict_table(1, 5, 7);

    ts.dump();

    std::cout << "Allocated bytes: " << springtail::TrackingAllocatorStats::get_instance()->get_allocated_bytes() << std::endl;
}

int main(void) {
    springtail::springtail_init();

    springtail::ThreadedTest<springtail::WriteCacheIndexTest, springtail::WriteCacheIndexTestRequest> tester(4);

    tester.start();

    return 0;
}