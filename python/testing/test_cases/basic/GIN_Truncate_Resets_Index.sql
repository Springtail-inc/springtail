## test
-- btree redesign final-3: UserMutableTable::truncate() (table_mgr.cc) is a fifth call site,
-- independent of the four final-2 already wired up, that iterates _secondary_indexes to reset
-- it on TRUNCATE TABLE -- but never touched _gist_indexes/_gin_indexes, so a READY GIN secondary
-- index's on-disk structure survived a TRUNCATE fully intact, still matching every pre-truncate
-- row. See phases/final-3.md for the full history.
--
-- Known limitation, named explicitly rather than silently assumed away, the same way
-- GIN_DML_After_Build.sql's header names its own: this test does *not* reliably isolate this
-- phase's table_mgr.cc fix from Indexer::_reconcile_index()'s pre-existing build-window catch-up
-- -- the ### sync directive only waits for WAL replication, not for the index's asynchronous
-- build+reconcile to reach READY. Independently confirmed (reverted this phase's table_mgr.cc fix
-- locally and reran): this integration test still passed at this table's trivial scale, for the
-- same reason GIN_DML_After_Build.sql's own test does. This file is end-to-end correctness
-- coverage for TRUNCATE composing with GIN qual routing; the regression oracle for this phase's
-- fix is src/pg_log_mgr/test/test_indexer.cc's Indexer_Test.Test_GinTruncateResetsReadyIndex (and
-- its GiST counterpart), which drives Indexer's real build/reconcile pipeline directly, calls
-- truncate() on a fresh MutableTable instance constructed strictly after the index's catalog
-- state is observed READY, and checks the result directly against GinReadStrategy -- a guarantee
-- this SQL-level harness has no directive to make, and *does* fail without the fix (reverted and
-- reran, per this project's default verification method).
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE gin_truncate_resets_index_test (
    id INT PRIMARY KEY,
    tags TEXT
);

INSERT INTO gin_truncate_resets_index_test (id, tags) VALUES
    (1, 'apple banana'), (2, 'banana cherry');

CREATE INDEX gin_truncate_resets_index_test_idx ON gin_truncate_resets_index_test USING gin (tags gin_trgm_ops);

### sync

TRUNCATE TABLE gin_truncate_resets_index_test;

INSERT INTO gin_truncate_resets_index_test (id, tags) VALUES (3, 'cherry fig');

## verify
### index_exists public gin_truncate_resets_index_test gin_truncate_resets_index_test_idx true

-- Only row 3 (inserted after TRUNCATE) should ever match -- rows 1/2 were truncated away. If the
-- pre-truncate GIN structure survived the TRUNCATE intact, this would also (wrongly) return row 2
-- ("banana cherry"), which shares the "cherry" trigram with row 3.
SELECT * FROM gin_truncate_resets_index_test WHERE tags % 'cherry' ORDER BY id;

-- Neither pre-truncate row's own terms ("apple", "banana") should match anything post-truncate.
SELECT * FROM gin_truncate_resets_index_test WHERE tags % 'apple' ORDER BY id;
SELECT * FROM gin_truncate_resets_index_test WHERE tags % 'banana' ORDER BY id;

SELECT * FROM gin_truncate_resets_index_test ORDER BY id;

## cleanup
DROP TABLE IF EXISTS gin_truncate_resets_index_test;
