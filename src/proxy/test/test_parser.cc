#include <vector>

#include <gtest/gtest.h>

#include <common/common_init.hh>
#include <common/logging.hh>
#include <proxy/parser.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

/** Tests to run.  In format: query string, is_read_safe, output name if any */
static const std::vector<std::tuple<std::string, bool, std::string>> tests = {
    {"SELECT 1", true, ""},
    {"SELECT * FROM x WHERE z = 2", true, ""},
    {"SELECT a FROM (SELECT abc from td WHERE y=2) as r where b in (SELECT 4 from tx)", true, ""},
    {"UPDATE foo SET bar = c FROM (SELECT c from abc WHERE baz = 2) as t", false, ""},
    {"INSERT INTO foo SELECT * FROM bar", false, ""},
    {"SELECT left(left('abc',1), 2)", true, ""},
    {"SELECT foo FROM bar FOR UPDATE", false, ""},
    {"SELECT nextval()", false, ""},
    {"SELECT extract(epoch from now())", true, ""},
    {"SET \"foo\"='bar'", true, "foo"},
    {"SET LOCAL foo='bar'", true, "foo"},
    {"SELECT foo FROM bar; UPDATE bar set foo = foo + 1", false, ""},
    {"COPY foo TO STDOUT", true, ""},
    {"PREPARE fooplan (int, text, bool, numeric) AS INSERT INTO foo VALUES($1, $2, $3, $4)", false, "fooplan"},
    {"PREPARE fooplan (int, text, bool, numeric) AS VALUES($1, $2, $3, $4)", true, "fooplan"},
    {"PREPARE foobar (int, text) AS INSERT INTO users (user_id, group_name) SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM users WHERE user_id = $1)", false, "foobar"},
    {"EXECUTE fooplan(1, 'Hunter Valley', 't', 200.0)", true, "fooplan"},
    {"SELECT a FROM b UNION SELECT x FROM y LIMIT 10", true, ""},
    {"ALTER TABLE foo ADD COLUMN bar INT", false, ""},
    {"EXPLAIN ANALYZE SELECT * FROM foo", false, ""},
    {"RESET ALL", true, ""},
    {"DECLARE myportal CURSOR FOR SELECT * FROM foo", true, "myportal"},
    {"DECLARE myportal CURSOR FOR SELECT * FROM foo FOR UPDATE", false, "myportal"},
    {"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", false, "serializable"},
    {"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", true, "repeatable read"},
    {"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", true, "repeatable read"},
    {"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", false, "serializable"},
    {"SET TRANSACTION SNAPSHOT 'name'", false, "name"},
    {"SET SESSION AUTHORIZATION 'paul'", true, "session_authorization"},
    {"LISTEN channel", false, "channel"},
    {"UNLISTEN channel", false, "channel"},
    {"NOTIFY channel 'payload'", false, ""},
    {"SAVEPOINT foo", true, "foo"},
    {"ROLLBACK TO SAVEPOINT foo", true, "foo"},
    {"RELEASE SAVEPOINT foo", true, "foo"},
};

// gtest function
TEST(ProxyParser_Test, TestParser)
{
    springtail_init();

    for (int i = 0; i < tests.size(); i++) {
        std::vector<Parser::StmtContextPtr> res = Parser::parse_query(std::get<0>(tests[i]),  [](const std::string &schema, const std::string &table) {
            return true;
        });

        bool is_readable = true;
        std::string name;
        for (auto &r : res) {
            SPDLOG_INFO("Query: {} is {}", std::get<0>(tests[i]), (r->is_read_safe ? "readable" : "NOT readable"));
            if (!r->is_read_safe) {
                is_readable = false;
            }
            if (!r->name.empty()) {
                name = r->name;
            }
        }

        bool expected_readable = std::get<1>(tests[i]);
        if (is_readable != expected_readable) {
            SPDLOG_ERROR("Mismatch in query expectations: expected={}, got={}", expected_readable, is_readable);
            Parser::dump_parse_tree(std::get<0>(tests[i]));
        }
        if (name != std::get<2>(tests[i])) {
            SPDLOG_INFO("Name mismatch: {} != {}", name, std::get<2>(tests[i]));
        }
        EXPECT_EQ(is_readable, expected_readable);
        EXPECT_EQ(name, std::get<2>(tests[i]));
    }

    springtail_shutdown();
}
