#include <vector>
#include <iostream>

#include <gtest/gtest.h>

#include <common/logging.hh>
#include <proxy/parser.hh>

using namespace springtail;

static const std::vector<std::string> tests = {
    "SELECT 1",
    "SELECT * FROM x WHERE z = 2",
    "SELECT a FROM (SELECT abc from td WHERE y=2) as r where b in (SELECT 4 from tx)",
    "UPDATE foo SET bar = c FROM (SELECT c from abc WHERE baz = 2) as t",
    "INSERT INTO foo SELECT * FROM bar",
    "SELECT left(left('abc',1), 2)",
    "SELECT foo FROM bar FOR UPDATE",
    "SELECT nextval()",
    "SELECT extract(epoch from now())",
    "SET \"foo\"='bar'",
    "SET LOCAL foo='bar'",
    "SELECT foo FROM bar; UPDATE bar set foo = foo + 1;",
    "COPY foo TO STDOUT",
    "PREPARE fooplan (int, text, bool, numeric) AS INSERT INTO foo VALUES($1, $2, $3, $4)",
    "PREPARE fooplan (int, text, bool, numeric) AS VALUES($1, $2, $3, $4)",
    "PREPARE foobar (int, text) AS INSERT INTO users (user_id, group_name) SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM users WHERE user_id = $1)",
    "EXECUTE fooplan(1, 'Hunter Valley', 't', 200.0)",
    "SELECT a FROM b UNION SELECT x FROM y LIMIT 10",
    "ALTER TABLE foo ADD COLUMN bar INT",
    "EXPLAIN ANALYZE SELECT * FROM foo",
    "RESET ALL", // maps to VariableSetStmt kind=VAR_RESET_ALL
    "DECLARE myportal CURSOR FOR SELECT * FROM foo",
    "DECLARE myportal CURSOR FOR SELECT * FROM foo FOR UPDATE",
    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    "SET TRANSACTION SNAPSHOT 'snapshot_name'",
    "SET SESSION AUTHORIZATION 'paul'",
    "LISTEN channel",
    "UNLISTEN channel",
    "SAVEPOINT foo",
    "ROLLBACK TO SAVEPOINT foo",
    "RELEASE SAVEPOINT foo",
};

int main(void)
{
    init_logging(LOG_ALL);

    for (int i = 0; i < tests.size(); i++) {
        std::cout << "\nQuery: " << tests[i] << std::endl;
        std::vector<Parser::StmtContextPtr> res = Parser::parse_query(tests[i]);
        Parser::dump_parse_tree(tests[i]);
        for (auto &r : res) {
            std::cout << "IS: " << (r->is_read_safe ? "readable\n" : "NOT readable\n") << std::endl;
        }
    }
    return 0;
}