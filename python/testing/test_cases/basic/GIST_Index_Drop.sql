## test
-- GiST strategy (btree redesign phase 10, round-1 review finding 1): a real "DROP INDEX" on a
-- GiST index must go through Indexer::_drop()'s GIST branch (create_gist_index_root(), truncate,
-- finalize) without crashing. This exercises the exact call site the round-1 review flagged as
-- newly added by this phase and missing the local try/catch its two sibling call sites
-- (Indexer::_build()'s own dispatch, Server::_upsert_index_name) already have around
-- index_access_method_from_string() -- GIST_Index_Build_And_Search.sql only ever drops the whole
-- *table* in cleanup, never the index directly, so it never actually ran this function's GIST
-- branch at all. This test drives a standalone DROP INDEX so that branch runs for real, proving
-- the round-1 fix's happy path (not just its new catch path, which needs a corrupt catalog row
-- to reach and is deliberately left as a traced-not-reproduced finding per the review's own
-- cost/benefit call) wasn't broken by wrapping it in a try/catch.
--
-- Note: "### index_exists" checks whether a row with this name has *ever* existed in
-- __pg_springtail_catalog.index_names, not its current state (that catalog is xid/lsn-versioned
-- history, confirmed directly from test_case.py's own query -- no state filter at all) -- it
-- cannot express "the index was dropped", so this test instead proves the drop succeeded
-- the same way INDEX_DDL_Alter_Add_Secondary_Index.sql's own precedent does: the DROP INDEX
-- statement itself completes without error (a crashed/hung daemon would fail the sync below),
-- and the table -- including its own primary-key index -- keeps answering ordinary DML/queries
-- correctly afterward.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE gist_index_drop_test (
    id INT PRIMARY KEY,
    val TEXT
);

INSERT INTO gist_index_drop_test (id, val) VALUES (1, 'alpha'), (2, 'beta');

CREATE INDEX gist_index_drop_test_idx ON gist_index_drop_test USING gist (val gist_trgm_ops);

## verify
-- Confirm the index reached the catalog and the table is queryable before the drop.
### index_exists public gist_index_drop_test gist_index_drop_test_idx true

SELECT * FROM gist_index_drop_test ORDER BY id;

## test
DROP INDEX gist_index_drop_test_idx;

INSERT INTO gist_index_drop_test (id, val) VALUES (3, 'gamma');

## verify
-- The drop completed (this sync, and everything after it, only runs if the prior "## test"
-- section's statements all succeeded -- a daemon crash during DROP INDEX would hang or error
-- here instead), and the table -- including its primary-key index, unaffected by the dropped
-- secondary one -- still answers queries normally afterward.
SELECT * FROM gist_index_drop_test ORDER BY id;

## cleanup
DROP TABLE IF EXISTS gist_index_drop_test;
