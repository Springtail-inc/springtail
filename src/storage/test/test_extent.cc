#include <gtest/gtest.h>

#include <common/common.hh>
#include <storage/extent.hh>
#include <storage/field.hh>

using namespace springtail;

namespace {
    /**
     * Framework for Extent testing.
     */
    class Extent_Test : public testing::Test {
    protected:
        void SetUp() override {
            springtail_init();

            // construct a schema for testing that includes inline data, variable data, bit-data and nullable fields
            std::vector<SchemaColumn> columns({
                    { "variable", 0, SchemaType::TEXT, 0, false },
                    { "fixed", 1, SchemaType::UINT64, 0, false },
                    { "bit", 2, SchemaType::BOOLEAN, 0, false },
                    { "nullable", 3, SchemaType::INT32, 0, true }
                });
            _schema = std::make_shared<ExtentSchema>(columns);

            _variable_f = _schema->get_mutable_field("variable");
            _fixed_f = _schema->get_mutable_field("fixed");
            _bit_f = _schema->get_mutable_field("bit");
            _nullable_f = _schema->get_mutable_field("nullable");
        }

        void TearDown() override {
            // remove any files created during the run
            IOMgr::get_instance()->remove("/tmp/test_extent_file");
        }

        ExtentSchemaPtr _schema;

        MutableFieldPtr _variable_f;
        MutableFieldPtr _fixed_f;
        MutableFieldPtr _bit_f;
        MutableFieldPtr _nullable_f;

        void
        _populate(Extent::Row row,
                  const std::string &variable,
                  uint64_t fixed,
                  bool bit,
                  int32_t nullable,
                  bool is_null)
        {
            _variable_f->set_text(row, variable);
            _fixed_f->set_uint64(row, fixed);
            _bit_f->set_bool(row, bit);
            if (is_null) {
                _nullable_f->set_null(row, true);
            } else {
                _nullable_f->set_int32(row, nullable);
            }
        }

        void
        _verify(const Extent::Row &row,
                const std::string &variable,
                uint64_t fixed,
                bool bit,
                int32_t nullable,
                bool is_null)
        {
            EXPECT_EQ(_variable_f->get_text(row).compare(variable), 0);
            EXPECT_EQ(_fixed_f->get_uint64(row), fixed);
            EXPECT_EQ(_bit_f->get_bool(row), bit);
            EXPECT_EQ(_nullable_f->is_null(row), is_null);
            if (!is_null) {
                EXPECT_EQ(_nullable_f->get_int32(row), nullable);
            }
        }

        ExtentPtr
        _create_and_populate(bool add_empty_variable=false)
        {
            // create an extent
            ExtentPtr extent = std::make_shared<Extent>(ExtentType{false}, 0, _schema->row_size());

            // fill it with data
            _populate(extent->append(),
                      "duplicate", 321837248973189LL, true, 15, false);
            _populate(extent->append(),
                      "duplicate", 15, false, 0, true);
            _populate(extent->append(),
                      "different", 4372895, true, 12, false);

			if (add_empty_variable) {
				_populate(extent->append(),
						  "", 4372896, true, 15, false);
			}

            return extent;
        }
    };

    TEST_F(Extent_Test, StartsEmpty) {
        // create an extent
        ExtentPtr extent = std::make_shared<Extent>(ExtentType{false}, 0, _schema->row_size());

        // make sure it starts empty
        ASSERT_TRUE(extent->empty());
    }

    TEST_F(Extent_Test, SerializeDeserialize) {
        // create a populated extent
        ExtentPtr extent = _create_and_populate(true);

        FieldPtr variable_f = _schema->get_field("variable");
        FieldPtr fixed_f = _schema->get_field("fixed");
        FieldPtr bit_f = _schema->get_field("bit");
        FieldPtr nullable_f = _schema->get_field("nullable");

        // serialize
        std::string temp = extent->serialize();
        
        // create an empty extent
        ExtentPtr new_extent = std::make_shared<Extent>(ExtentType{false}, 0, _schema->row_size());
        new_extent->deserialize(temp);

        // make sure that the data matches exepctations
        EXPECT_FALSE(new_extent->empty());
        EXPECT_EQ(_schema->row_size(), new_extent->row_size());
        EXPECT_TRUE(new_extent->byte_count() > 0);
        EXPECT_EQ(new_extent->row_count(), extent->row_count());
    }

    TEST_F(Extent_Test, WriteAndRead) {
        // create a populated extent
        ExtentPtr extent = _create_and_populate(true);

        FieldPtr variable_f = _schema->get_field("variable");
        FieldPtr fixed_f = _schema->get_field("fixed");
        FieldPtr bit_f = _schema->get_field("bit");
        FieldPtr nullable_f = _schema->get_field("nullable");

        // make sure that all of the helper functions work as expected
        EXPECT_FALSE(extent->empty());
        EXPECT_EQ(_schema->row_size(), extent->row_size());
        EXPECT_TRUE(extent->byte_count() > 0);
        EXPECT_EQ(extent->row_count(), 4);

        // check that the iterator works as expected
        auto i = extent->begin();

        EXPECT_EQ(variable_f->get_text(*i).compare("duplicate"), 0);
        EXPECT_EQ(fixed_f->get_uint64(*i), 321837248973189LL);
        EXPECT_TRUE(bit_f->get_bool(*i));
        EXPECT_FALSE(nullable_f->is_null(*i));
        EXPECT_EQ(nullable_f->get_int32(*i), 15);

        ++i; // test increment
        i += 3; // test addition
        EXPECT_EQ(i, extent->end()); // make sure we made it to the end()

        // check that the data of the second entry matches
        {
            auto row = extent->at(1);
            EXPECT_EQ(variable_f->get_text(*row).compare("duplicate"), 0);
            EXPECT_EQ(fixed_f->get_uint64(*row), 15);
            EXPECT_FALSE(bit_f->get_bool(*row));
            EXPECT_TRUE(nullable_f->is_null(*row));
        }

        // check that the data of the third entry matches
        {
            auto row = extent->at(2);
            EXPECT_EQ(variable_f->get_text(*row).compare("different"), 0);
            EXPECT_EQ(fixed_f->get_uint64(*row), 4372895);
            EXPECT_TRUE(bit_f->get_bool(*row));
            EXPECT_FALSE(nullable_f->is_null(*row));
            EXPECT_EQ(nullable_f->get_int32(*row), 12);
        }

        // check the data of the last entry matches
        Extent::Row back_row = extent->back();
        EXPECT_EQ(variable_f->get_text(back_row).compare(""), 0);
        EXPECT_EQ(fixed_f->get_uint64(back_row), 4372896);
        EXPECT_TRUE(bit_f->get_bool(back_row));
        EXPECT_FALSE(nullable_f->is_null(back_row));
        EXPECT_EQ(nullable_f->get_int32(back_row), 15);
    }

    TEST_F(Extent_Test, WriteAndReadViaDisk) {
        // create a populated extent
        ExtentPtr extent = _create_and_populate(true);

        // construct a file handle
        auto handle = IOMgr::get_instance()->open("/tmp/test_extent_file",
                                                  IOMgr::IO_MODE::WRITE, true);

        // write it to disk
        auto future = extent->async_flush(handle);
        auto &&write_resp = future.get();

        // read it from disk
        auto read_resp = handle->read(write_resp->offset);

        // read the data back
        ExtentPtr disk_extent = std::make_shared<Extent>(read_resp->data);

        // verify the data against the data we wrote
        ASSERT_EQ(extent->row_count(), disk_extent->row_count());
        ASSERT_EQ(extent->byte_count(), disk_extent->byte_count());

        auto fs = _schema->get_fields();
        auto j = disk_extent->begin();
        for (auto i = extent->begin(); i != extent->end(); ++i, ++j)
        {
            // compare less_than() both directions to check for equality
            ASSERT_FALSE(FieldTuple(fs, *i).less_than(FieldTuple(fs, *j)));
            ASSERT_FALSE(FieldTuple(fs, *j).less_than(FieldTuple(fs, *i)));
        }
    }

    TEST_F(Extent_Test, InsertAndRemove) {
        // create a populated extent
        ExtentPtr extent = _create_and_populate();

        // insert rows into the middle of the extent
        auto &&i = extent->begin();
        ++i;

        _populate(extent->insert(i),
                  "different", 6666, false, 7777, false);

        // read the data back and verify it
        i = extent->begin();
        _verify(*i, "duplicate", 321837248973189LL, true, 15, false);

        ++i;
        _verify(*i, "different", 6666, false, 7777, false);

        ++i;
        _verify(*i, "duplicate", 15, false, 0, true);

        ++i;
        _verify(*i, "different", 4372895, true, 12, false);

        ++i;
        ASSERT_EQ(i, extent->end());

        // remove other rows from the extent
        extent->remove(extent->begin());

        // read the data back and verify it
        i = extent->begin();
        _verify(*i, "different", 6666, false, 7777, false);

        ++i;
        _verify(*i, "duplicate", 15, false, 0, true);

        ++i;
        _verify(*i, "different", 4372895, true, 12, false);

        ++i;
        ASSERT_EQ(i, extent->end());
    }

    TEST_F(Extent_Test, WriteAndSplit) {
        // create a populated extent
        ExtentPtr extent = _create_and_populate();

        // split it
        auto pair = extent->split(_schema);

        // verify it against the two extents
        ASSERT_EQ(extent->row_count(),
                  pair.first->row_count() + pair.second->row_count());

        // verify that both extents have rows
        ASSERT_TRUE(pair.first->row_count() > 0);
        ASSERT_TRUE(pair.second->row_count() > 0);


        auto fs = _schema->get_fields();
        bool on_first = true;
        auto &&i = pair.first->begin();
        for (auto &&row : *extent) {
            // check if we need to switch extents
            if (on_first && i == pair.first->end()) {
                on_first = false;
                i = pair.second->begin();
            }

            ASSERT_FALSE(FieldTuple(fs, row).less_than(FieldTuple(fs, *i)));
            ASSERT_FALSE(FieldTuple(fs, *i).less_than(FieldTuple(fs, row)));

            ++i;
        }
    }

}
