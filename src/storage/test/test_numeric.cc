#include <gtest/gtest.h>

#include <common/init.hh>
#include <storage/numeric_utils.hh>

using namespace springtail;

namespace {
    class Numeric_Test : public testing::Test
    {
        void SetUp() override
        {
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            springtail_init_test(runners, LOG_ALL);
        }
        void TearDown() override
        {
            springtail_shutdown();
        }

    };

    TEST_F(Numeric_Test, CreateNumericData)
    {
        std::string one("1");
        std::string two("2");
        std::string three("3");
        std::string four("4");
        std::string five("5");
        std::string six("6");
        std::string seven("7");
        std::string eight("8");
        std::string nine("9");
        std::string ten("10");

        numeric::Numeric num_1 = numeric::NumericData::numeric_from_string(one, 0);
        numeric::Numeric num_2 = numeric::NumericData::numeric_from_string(two, 0);
        numeric::Numeric num_3 = numeric::NumericData::numeric_from_string(three, 0);
        numeric::Numeric num_4 = numeric::NumericData::numeric_from_string(four, 0);
        numeric::Numeric num_5 = numeric::NumericData::numeric_from_string(five, 0);
        numeric::Numeric num_6 = numeric::NumericData::numeric_from_string(six, 0);
        numeric::Numeric num_7 = numeric::NumericData::numeric_from_string(seven, 0);
        numeric::Numeric num_8 = numeric::NumericData::numeric_from_string(eight, 0);
        numeric::Numeric num_9 = numeric::NumericData::numeric_from_string(nine, 0);
        numeric::Numeric num_10 = numeric::NumericData::numeric_from_string(ten, 0);

        num_1->dump("num_1");
        num_2->dump("num_2");
        num_3->dump("num_3");
        num_4->dump("num_4");
        num_5->dump("num_5");
        num_6->dump("num_6");
        num_7->dump("num_7");
        num_8->dump("num_8");
        num_9->dump("num_9");
        num_10->dump("num_10");

        EXPECT_EQ(numeric::NumericData::cmp(num_1, num_1), 0);
        EXPECT_EQ(numeric::NumericData::cmp(num_1, num_2), -1);
        EXPECT_EQ(numeric::NumericData::cmp(num_2, num_1), 1);

        numeric::NumericData::free_numeric(num_1);
        numeric::NumericData::free_numeric(num_2);
        numeric::NumericData::free_numeric(num_3);
        numeric::NumericData::free_numeric(num_4);
        numeric::NumericData::free_numeric(num_5);
        numeric::NumericData::free_numeric(num_6);
        numeric::NumericData::free_numeric(num_7);
        numeric::NumericData::free_numeric(num_8);
        numeric::NumericData::free_numeric(num_9);
        numeric::NumericData::free_numeric(num_10);
    }
} // namespace