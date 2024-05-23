#include <gtest/gtest.h>

#include <common/common.hh>
#include <garbage_collector/gc_extent_mapper.hh>

using namespace springtail;

namespace {

    /**
     * Framework for testing the gc::ExtentMapper.
     */
    class ExtentMapper_Test : public testing::Test {
    protected:
        void SetUp() override {
            springtail_init();
            _extent_mapper = std::make_shared<gc::ExtentMapper>();
        }

        void TearDown() override {

        }

        std::shared_ptr<gc::ExtentMapper> _extent_mapper;
        uint64_t _lookup_eid;
        std::map<uint64_t, uint64_t> _write_cache;

        // simulate GC-1 step using the current "lookup" extent ID and the provided XID
        void
        _gc1(uint64_t tid, uint64_t xid)
        {
            _extent_mapper->set_lookup(tid, xid, _lookup_eid);
            _write_cache[xid] = _lookup_eid;
        }

        std::vector<uint64_t>
        _gc2(uint64_t tid, uint64_t xid, int count = 1)
        {
            auto lookup_eid = _write_cache[xid];
            auto &&m = _extent_mapper->forward_map(tid, xid, lookup_eid);

            uint64_t update_eid = (m.empty())
                ? lookup_eid
                : m.back();

            std::vector<uint64_t> new_eids;
            for (int i = 1; i <= count; ++i) {
                new_eids.push_back(update_eid + i);
            }

            _extent_mapper->add_mapping(tid, xid, update_eid, new_eids);

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
        ASSERT_TRUE(m.empty());

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
#if 0
        _extent_mapper->set_lookup(1, 100);
        _extent_mapper->set_lookup(2, 100);
        _extent_mapper->set_lookup(4, 100);
        _extent_mapper->set_lookup(7, 100);

        auto &&m = _extent_mapper->forward_map(2, 100);
        ASSERT_TRUE(m.empty());

        _extent_mapper->add_mapping(2, 100, { 101 });

        m = _extent_mapper->forward_map(4, 100);
        ASSERT_TRUE(m.size() == 1);
        ASSERT_TRUE(m[0] == 101);

        _extent_mapper->add_mapping(4, 101, { 102 });

        m = _extent_mapper->forward_map(7, 100);
        ASSERT_TRUE(m.size() == 1);
        ASSERT_TRUE(m[0] == 102);

        m = _extent_mapper->reverse_map(3, 6, 101);
        ASSERT_TRUE(m.size() == 1);
        ASSERT_TRUE(m[0] == 100);

        _extent_mapper->add_mapping(7, 102, { 103, 107 });

        m = _extent_mapper->reverse_map(5, 7, 102);
        ASSERT_TRUE(m.size() == 1);
        ASSERT_TRUE(m[0] == 100);

        _extent_mapper->set_lookup(8, 100);
        _extent_mapper->set_lookup(9, 107);
        _extent_mapper->set_lookup(10, 107);


        m = _extent_mapper->forward_map(8, 100);
        ASSERT_TRUE(m.size() == 2);
        ASSERT_TRUE(m[0] == 103);
        ASSERT_TRUE(m[1] == 107);

        _extent_mapper->add_mapping(8, 107, { 108 });

        m = _extent_mapper->forward_map(9, 107);
        ASSERT_TRUE(m.size() == 1);
        ASSERT_TRUE(m[0] == 108);

        _extent_mapper->add_mapping(9, 108, { 109 });
        _extent_mapper->add_mapping(10, 109, { 110, 115 });

        m = _extent_mapper->reverse_map(9, 10, 108);
        ASSERT_TRUE(m.size() == 1);
        ASSERT_TRUE(m[0] == 100);

        _extent_mapper->expire(9);

        m = _extent_mapper->reverse_map(9, 10, 108);
        ASSERT_TRUE(m.size() == 0);

        _extent_mapper->set_lookup(11, 115);
        _extent_mapper->set_lookup(12, 115);
        _extent_mapper->set_lookup(13, 115);

        _extent_mapper->add_mapping(11, 115, { 116 });
        _extent_mapper->add_mapping(12, 116, { 117 });

        m = _extent_mapper->forward_map(13, 115);
        ASSERT_TRUE(m.size() == 1);
        ASSERT_TRUE(m[0] == 117);

        _extent_mapper->add_mapping(13, 117, { 118 });
        _extent_mapper->expire(13);
#endif        
    }

}
