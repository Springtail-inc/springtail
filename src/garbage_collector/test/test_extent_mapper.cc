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
    };

    /** Tests the basic functionality of the ExtentMapper */
    TEST_F(ExtentMapper_Test, Basic) {
        _extent_mapper->set_lookup(1, 100);
        _extent_mapper->set_lookup(2, 100);
        _extent_mapper->set_lookup(4, 100);
        _extent_mapper->set_lookup(7, 100);

        auto &&m = _extent_mapper->forward_map(2, 100);
        assert(m.empty());

        _extent_mapper->add_mapping(2, 100, { 101 });

        m = _extent_mapper->forward_map(4, 100);
        assert(m.size() == 1);
        assert(m[0] == 101);

        _extent_mapper->add_mapping(4, 101, { 102 });

        m = _extent_mapper->forward_map(7, 100);
        assert(m.size() == 1);
        assert(m[0] == 102);

        _extent_mapper->add_mapping(7, 102, { 103, 107 });

        _extent_mapper->set_lookup(8, 100);
        _extent_mapper->expire(7);

        _extent_mapper->set_lookup(9, 107);
        _extent_mapper->set_lookup(10, 107);

        m = _extent_mapper->forward_map(8, 100);
        assert(m.size() == 2);
        assert(m[0] == 103);
        assert(m[1] == 107);

        _extent_mapper->add_mapping(8, 107, { 108 });
        _extent_mapper->expire(8);

        m = _extent_mapper->forward_map(9, 107);
        assert(m.size() == 1);
        assert(m[0] == 108);

        _extent_mapper->add_mapping(9, 108, { 109 });
        _extent_mapper->add_mapping(10, 109, { 110, 115 });

        _extent_mapper->expire(10);

        _extent_mapper->set_lookup(11, 115);
        _extent_mapper->set_lookup(12, 115);
        _extent_mapper->set_lookup(13, 115);

        _extent_mapper->add_mapping(11, 115, { 116 });
        _extent_mapper->add_mapping(12, 116, { 117 });
        _extent_mapper->add_mapping(13, 117, { 118 });
    }

    /** Tests multi-threaded functionality of the ExtentMapper */
    TEST_F(ExtentMapper_Test, Threaded) {
        
    }

}
