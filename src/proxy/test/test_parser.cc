#include <vector>
#include <iostream>

#include <gtest/gtest.h>

#include <common/logging.hh>
#include <proxy/parser.hh>

using namespace springtail;

static const std::vector<std::pair<std::string, bool>> tests = {
    {"SELECT 1", true},
    {"SELECT * FROM x WHERE z = 2", true},
    {"SELECT a FROM (SELECT abc from td WHERE y=2) as r where b in (SELECT 4 from tx)", true},
    {"UPDATE foo SET bar = c FROM (SELECT c from abc WHERE baz = 2) as t", false},
    {"INSERT INTO foo SELECT * FROM bar", false},
    {"SELECT left(left('abc',1), 2)", true},
    {"SELECT foo FROM bar FOR UPDATE", false},
    {"SELECT nextval()", false},
    {"SELECT extract(epoch from now())", true},
    {"SET \"foo\"='bar'", true},
    {"SET LOCAL foo='bar'", true},
    {"SELECT foo FROM bar; UPDATE bar set foo = foo + 1", false},
    {"COPY foo TO STDOUT", true},
    {"PREPARE fooplan (int, text, bool, numeric) AS INSERT INTO foo VALUES($1, $2, $3, $4)", false},
    {"PREPARE fooplan (int, text, bool, numeric) AS VALUES($1, $2, $3, $4)", true},
    {"PREPARE foobar (int, text) AS INSERT INTO users (user_id, group_name) SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM users WHERE user_id = $1)", false},
    {"EXECUTE fooplan(1, 'Hunter Valley', 't', 200.0)", true},
    {"SELECT a FROM b UNION SELECT x FROM y LIMIT 10", true},
    {"ALTER TABLE foo ADD COLUMN bar INT", false},
    {"EXPLAIN ANALYZE SELECT * FROM foo", false},
    {"RESET ALL", true},
    {"DECLARE myportal CURSOR FOR SELECT * FROM foo", true},
    {"DECLARE myportal CURSOR FOR SELECT * FROM foo FOR UPDATE", false},
    {"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", false},
    {"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", true},
    {"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", true},
    {"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", false},
    {"SET TRANSACTION SNAPSHOT 'name'", false},
    {"SET SESSION AUTHORIZATION 'paul'", true},
    {"LISTEN channel", false},
    {"UNLISTEN channel", false},
    {"NOTIFY 'channel', 'payload'", false},
    {"SAVEPOINT foo", true},
    {"ROLLBACK TO SAVEPOINT foo", true},
    {"RELEASE SAVEPOINT foo", true},
};
// gtest function
TEST(ProxyParser_Test, TestParser)
{
    init_logging(LOG_ALL);

    for (int i = 0; i < tests.size(); i++) {
        std::vector<Parser::StmtContextPtr> res = Parser::parse_query(tests[i].first);

        bool is_readable = true;
        for (auto &r : res) {
            SPDLOG_INFO("Query: {} is {}", tests[i].first, (r->is_read_safe ? "readable" : "NOT readable"));
            if (!r->is_read_safe) {
                is_readable = false;
            }
        }

        if (is_readable != tests[i].second) {
            SPDLOG_ERROR("Mismatch in query expectations: expected={}, got={}", tests[i].second, is_readable);
            Parser::dump_parse_tree(tests[i].first);
        }
        ASSERT_EQ(is_readable, tests[i].second);
    }
}
