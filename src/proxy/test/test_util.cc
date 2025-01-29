#include <gtest/gtest.h>

#include <proxy/util.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

namespace {

    TEST(UtilTest, QuotePostgresValue) {
        std::vector<std::pair<std::string, std::string>> test_values = {
            {"postgres_verbose", "postgres_verbose"},
            {"'already_quoted'", "'already_quoted'"},
            {"\"double_quoted\"", "\"double_quoted\""},
            {"123", "123"},
            {"some value with spaces", "'some value with spaces'"},
            {"contains'single'quotes", "'contains''single''quotes'"},
            {"contains\"double\"quotes", "'contains\"double\"quotes'"},
            {"safe_identifier", "safe_identifier"}
        };

        for (const auto &test : test_values) {
            EXPECT_EQ(util::quote_postgres_value(test.first), test.second);
        }
    }

    TEST(UtilTest, IsValidPostgresValue) {
        std::vector<std::pair<std::string, bool>> test_values = {
            {"postgres_verbose", true},
            {"'postgres_verbose'", true},
            {"\"postgres_verbose\"", true},
            {"123", true},
            {"on", true},
            {"false", true},
            {"'value with spaces'", true},
            {"'value'with'quote'", false},
            {"-123.45", true},
            {"some;value", false},
            {"'value''with''quotes'", true},
        };

        for (const auto &test : test_values) {
            EXPECT_EQ(util::is_valid_postgres_value(test.first), test.second);
        }
    }

    TEST(UtilTest, IsValidPostgresKey) {
        std::vector<std::pair<std::string, bool>> test_keys = {
            {"work_mem", true},
            {"search_path", true},
            {"pg_stat_statements.max", true},
            {"_customKey123", true},
            {"123invalid", false},
            {"log-statement", false},
            {"key@name", false},
            {"too_long_key_name_that_exceeds_sixty_three_characters_in_length_is_invalid", false}
        };

        for (const auto &test : test_keys) {
            EXPECT_EQ(util::is_valid_postgres_key(test.first), test.second);
        }
    }

    TEST(UtilTest, ParseOptions) {
        std::vector<std::pair<std::string, std::map<std::string, std::string>>> test_cases = {
            {"-c work_mem=4MB -c search_path=public", {{"work_mem", "4MB"}, {"search_path", "public"}}},
            {"-c key1=value1 -c key2=value2", {{"key1", "value1"}, {"key2", "value2"}}},
            {"-c key_with_spaces='value with spaces'", {{"key_with_spaces", "'value with spaces'"}}},
            {"-c key_with_quotes='value''with''quotes'", {{"key_with_quotes", "'value''with''quotes'"}}},
            {"-c key_with_double_quotes=\"value with double quotes\"", {{"key_with_double_quotes", "\"value with double quotes\""}}},
            {" -c key1=value1 -c key2=value2 -c key3=value3", {{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}}},
            {"-c key1=value1", {{"key1", "value1"}}}
        };

        for (const auto &test : test_cases) {
            EXPECT_EQ(util::parse_options(test.first), test.second);
        }
    }

} // namespace