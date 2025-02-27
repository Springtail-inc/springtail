#include <gtest/gtest.h>

#include <common/common_init.hh>
#include <write_cache/extent_mapper.hh>

using namespace springtail;

namespace {

    /**
     * Framework for testing the ExtentMapper.
     */
    class ExtentMapper_Test : public testing::Test {
    protected:
        void SetUp() override {
            springtail_init();
            _extent_mapper = ExtentMapper::get_instance(1);

            _gc1_xid = 1;
            _gc2_xid = 1;

            _query_done = false;
        }

        void TearDown() override {
            springtail_shutdown();
        }

        ExtentMapper *_extent_mapper;
        uint64_t _lookup_eid;

        boost::mutex _write_cache_mutex;
        std::map<uint64_t, uint64_t> _write_cache;

        std::atomic<uint64_t> _gc1_xid;
        std::atomic<uint64_t> _gc2_xid;
        std::atomic<bool> _query_done;

        boost::mutex _cv_mutex;
        boost::condition_variable _gc2_cv;

        // simulate GC-1 step using the current "lookup" extent ID and the provided XID
        void
        _gc1(uint64_t tid, uint64_t xid)
        {
            _extent_mapper->set_lookup(tid, xid, _lookup_eid);

            {
                boost::unique_lock lock(_write_cache_mutex);
                _write_cache[xid] = _lookup_eid;
            }
        }

        std::vector<uint64_t>
        _gc2(uint64_t tid, uint64_t xid, int count = 1)
        {
            uint64_t lookup_eid;
            {
                boost::unique_lock lock(_write_cache_mutex);
                lookup_eid = _write_cache[xid];
            }

            auto &&m = _extent_mapper->forward_map(tid, xid, lookup_eid);

            uint64_t update_eid = (m.empty())
                ? lookup_eid
                : m.back();

            std::vector<uint64_t> new_eids;
            for (int i = 1; i <= count; ++i) {
                new_eids.push_back(update_eid + i);
            }

            _extent_mapper->add_mapping(tid, xid, update_eid, new_eids);
            _gc2_xid = xid;

            return m;
        }
    };

    /** Tests the basic functionality of the ExtentMapper */
    TEST_F(ExtentMapper_Test, Basic) {
        uint64_t tid = 1;
        _lookup_eid = 100;

        _gc1(tid, 1);
        _gc1(tid, 2);
        _gc1(tid, 3);
        _gc1(tid, 4);

        auto &&m = _gc2(tid, 1);
        ASSERT_EQ(m.size(), 1);
        ASSERT_EQ(m[0], 100);

        m = _gc2(tid, 2);
        ASSERT_EQ(m.size(), 1);
        ASSERT_EQ(m[0], 101);

        _lookup_eid = 102;

        _gc1(tid, 5);

        m = _gc2(tid, 3);
        ASSERT_EQ(m.size(), 1);
        ASSERT_EQ(m[0], 102);

        m = _gc2(tid, 4);
        ASSERT_EQ(m.size(), 1);
        ASSERT_EQ(m[0], 103);

        _gc1(tid, 6);
        _gc1(tid, 7);
        _gc1(tid, 8);

        m = _gc2(tid, 5);
        ASSERT_EQ(m.size(), 1);
        ASSERT_EQ(m[0], 104);

        _lookup_eid = 105;

        m = _gc2(tid, 6, 2);
        ASSERT_EQ(m.size(), 1);
        ASSERT_EQ(m[0], 105);

        m = _gc2(tid, 7);
        ASSERT_EQ(m.size(), 2);
        ASSERT_EQ(m[0], 106);
        ASSERT_EQ(m[1], 107);

        m = _extent_mapper->reverse_map(tid, 1, 7, 101);
        ASSERT_EQ(m.size(), 2);
        ASSERT_EQ(m[0], 100);
        ASSERT_EQ(m[1], 102);

        _gc1(tid, 9);
        _gc1(tid, 10);

        m = _extent_mapper->reverse_map(tid, 6, 9, 106);
        ASSERT_EQ(m.size(), 2);
        ASSERT_EQ(m[0], 102);
        ASSERT_EQ(m[1], 105);

        _extent_mapper->expire(tid, 6);

        m = _extent_mapper->reverse_map(tid, 6, 7, 106);
        ASSERT_EQ(m.size(), 1);
        ASSERT_EQ(m[0], 102);
    }

    /** Tests multi-threaded functionality of the ExtentMapper */
    TEST_F(ExtentMapper_Test, Threaded) {

        // create three threads:
        // 1) acts like GC-1, performing set_lookup(); occassionally moves the lookup XID forward and does expire()
        // 2) acts like GC-2, performing forward_map() + add_mapping()
        // 3) acts like query nodes, performing reverse_map()

        std::thread gc1([this]() {
            uint64_t lookup_xid = 1;

            while (_gc1_xid < 2000) {
                // perform GC-1 step
                _gc1(1, _gc1_xid);

                // signal the gc2 that a step is completed
                ++_gc1_xid;
                _gc2_cv.notify_one();

                // occasionally move the lookup XID forward and do an expire()
                if (std::rand() % 50 == 0) {
                    // select an XID between the current lookup_xid and the GC-2 xid
                    uint64_t step = _gc2_xid - lookup_xid;
                    lookup_xid += step;
                    {
                        boost::unique_lock lock(_write_cache_mutex);
                        _lookup_eid = _write_cache[lookup_xid];
                    }

                    // perform the expire()
                    _extent_mapper->expire(1, lookup_xid);
                }

                std::this_thread::yield();
            }
        });

        std::thread gc2([this]() {
            while (_gc2_xid < 2000) {
                // wait until we can perform the gc2 step
                if (_gc2_xid == _gc1_xid) {
                    boost::unique_lock lock(_cv_mutex);
                    _gc2_cv.wait(lock);
                }

                // perform the gc2 step
                _gc2(1, _gc2_xid, (std::rand() % 3) + 1);

                // move to the next XID
                ++_gc2_xid;

                std::this_thread::yield();
            }

            _query_done = true;
        });

        std::thread query([this]() {
            using namespace std::chrono_literals;

            while (!_query_done) {
                // perform a reverse_map() operation to the latest gc1 XID
                uint64_t access_xid = _gc2_xid;
                uint64_t lookup_eid;
                {
                    boost::unique_lock lock(_write_cache_mutex);
                    lookup_eid = _write_cache[access_xid];
                }

                _extent_mapper->reverse_map(1, access_xid - 4, _gc1_xid, lookup_eid);

                std::this_thread::sleep_for(100ms);
            }
        });

        gc1.join();
        gc2.join();
        query.join();
    }

}
