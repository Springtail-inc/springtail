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

        std::shared_ptr<numeric::NumericData> num_1 = numeric::NumericData::from_string(one, 0);
        std::shared_ptr<numeric::NumericData> num_2 = numeric::NumericData::from_string(two, 0);
        std::shared_ptr<numeric::NumericData> num_3 = numeric::NumericData::from_string(three, 0);
        std::shared_ptr<numeric::NumericData> num_4 = numeric::NumericData::from_string(four, 0);
        std::shared_ptr<numeric::NumericData> num_5 = numeric::NumericData::from_string(five, 0);
        std::shared_ptr<numeric::NumericData> num_6 = numeric::NumericData::from_string(six, 0);
        std::shared_ptr<numeric::NumericData> num_7 = numeric::NumericData::from_string(seven, 0);
        std::shared_ptr<numeric::NumericData> num_8 = numeric::NumericData::from_string(eight, 0);
        std::shared_ptr<numeric::NumericData> num_9 = numeric::NumericData::from_string(nine, 0);
        std::shared_ptr<numeric::NumericData> num_10 = numeric::NumericData::from_string(ten, 0);

        num_1->dump("num_1 = ");
        num_2->dump("num_2 = ");
        num_3->dump("num_3 = ");
        num_4->dump("num_4 = ");
        num_5->dump("num_5 = ");
        num_6->dump("num_6 = ");
        num_7->dump("num_7 = ");
        num_8->dump("num_8 = ");
        num_9->dump("num_9 = ");
        num_10->dump("num_10 = ");

        EXPECT_EQ(numeric::NumericData::cmp(num_1, num_1), 0);
        EXPECT_EQ(numeric::NumericData::cmp(num_1, num_2), -1);
        EXPECT_EQ(numeric::NumericData::cmp(num_2, num_1), 1);

        std::string pos_frac_number(".00000055260225961552");
        std::string neg_frac_number("-.00000055260225961552");

        std::shared_ptr<numeric::NumericData> pos_frac_numeric = numeric::NumericData::from_string(pos_frac_number, 0);
        std::shared_ptr<numeric::NumericData> neg_frac_numeric = numeric::NumericData::from_string(neg_frac_number, 0);

        pos_frac_numeric->dump("positive fractional number numeric = ");
        neg_frac_numeric->dump("negative fractional number numeric = ");

        EXPECT_EQ(numeric::NumericData::cmp(neg_frac_numeric, pos_frac_numeric), -1);
        EXPECT_EQ(numeric::NumericData::cmp(pos_frac_numeric, neg_frac_numeric), 1);

        std::string positive_big_number("85243.39540024977626076239847863600785982737155858270959890014613035727868293618673807776733416230953723818527101593495895350807775607346277892835514324320448949370623441059033804864158715021903312693889518990256881059434042443507529601095150710777634743301398926463888783847290873199395304998050753365215426971278237920063435565949203678024225270616295573678510929020831006146661747271783837653203039829647102027431761129518881525935216608429897041525858540380754759125150233053469999022855035");
        std::string negative_big_number("-85243.39540024977626076239847863600785982737155858270959890014613035727868293618673807776733416230953723818527101593495895350807775607346277892835514324320448949370623441059033804864158715021903312693889518990256881059434042443507529601095150710777634743301398926463888783847290873199395304998050753365215426971278237920063435565949203678024225270616295573678510929020831006146661747271783837653203039829647102027431761129518881525935216608429897041525858540380754759125150233053469999022855035");

        std::shared_ptr<numeric::NumericData> positive_big_numeric = numeric::NumericData::from_string(positive_big_number, 0);
        std::shared_ptr<numeric::NumericData> negative_big_numeric = numeric::NumericData::from_string(negative_big_number, 0);

        positive_big_numeric->dump("positive big number numeric = ");
        negative_big_numeric->dump("negative big number numeric = ");

        EXPECT_EQ(numeric::NumericData::cmp(negative_big_numeric, positive_big_numeric), -1);
        EXPECT_EQ(numeric::NumericData::cmp(positive_big_numeric, negative_big_numeric), 1);
    }
} // namespace