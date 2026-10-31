## test
-- btree redesign final-2: a READY GIN index must stay current with ordinary INSERT/UPDATE/DELETE
-- issued *after* CREATE INDEX, not just rows present at build time -- the gap phase 10/11 each
-- named explicitly and no later phase closed (see phases/final-2.md for the full history).
-- GIN_Index_Build_And_Search.sql/GIN_Trgm_Similarity_Query_Routing.sql deliberately insert
-- *before* CREATE INDEX specifically to avoid exercising this gap (see each file's own header
-- comment, now updated to point here instead of calling it deferred); this file is the
-- insert/update/delete-*after*-CREATE INDEX case those comments point to, and also exercises the
-- Open Question this phase's brief raised explicitly: does phase 12's qual routing
-- (_try_init_gin_scan(), the "%" similarity operator) still compose correctly against rows that
-- arrived/changed/disappeared after the index went READY? The dual-compare "WHERE tags % ..."
-- queries below are a real correctness oracle against actual Postgres (test_case.py's verify()).
--
-- Known limitation, named explicitly rather than silently assumed away: this test does *not*
-- reliably isolate this phase's fix (MutableTable's new per-row dispatch) from
-- Indexer::_reconcile_index()'s pre-existing build-window catch-up -- the ### sync directive only
-- waits for WAL replication to reach this point, not for the index's asynchronous build+reconcile
-- to reach READY, so this file's own DML below can race either path. Independently confirmed
-- (reverted this phase's mutable_table.cc fix locally and reran): this integration test still
-- passed, because the race resolved in reconciliation's favor every time at this table's trivial
-- scale -- so this file is end-to-end correctness coverage for the DML-after-CREATE-INDEX
-- scenario (and for routing composing with it), not the regression oracle for this phase's fix.
-- That oracle is src/pg_log_mgr/test/test_indexer.cc's Indexer_Test.Test_GinDmlMaintainsReadyIndex
-- (and its GiST counterpart), which drives Indexer's real build/reconcile pipeline directly and
-- *does* fail without the fix (reverted and reran, per this project's default verification
-- method) by forcing the DML onto a fresh MutableTable instance constructed strictly after the
-- index's catalog state is observed READY -- a guarantee this SQL-level harness has no directive
-- to make.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE gin_dml_after_build_test (
    id INT PRIMARY KEY,
    tags TEXT
);

INSERT INTO gin_dml_after_build_test (id, tags) VALUES
    (1, 'apple banana'), (2, 'banana cherry');

CREATE INDEX gin_dml_after_build_test_idx ON gin_dml_after_build_test USING gin (tags gin_trgm_ops);

### sync

-- Ordinary DML issued after the index above is created -- the scenario this phase's fix targets,
-- subject to this file's own header comment above on what this specific test can and can't
-- isolate.
INSERT INTO gin_dml_after_build_test (id, tags) VALUES (3, 'apple fig');
UPDATE gin_dml_after_build_test SET tags = 'grape date' WHERE id = 2;
DELETE FROM gin_dml_after_build_test WHERE id = 1;

## verify
### index_exists public gin_dml_after_build_test gin_dml_after_build_test_idx true

-- Row 1 (the only original "apple banana" row) was deleted; row 3 ("apple fig") is the only
-- surviving "apple" match, and it was inserted *after* the index was already READY.
SELECT * FROM gin_dml_after_build_test WHERE tags % 'apple' ORDER BY id;

-- Row 2's text was updated away from "banana cherry" to "grape date", and row 1 (which never
-- had "banana" either) was deleted -- no remaining row matches "banana".
SELECT * FROM gin_dml_after_build_test WHERE tags % 'banana' ORDER BY id;

-- Row 2's new content is findable under its *new* value -- confirms the UPDATE's remove-old-
-- then-insert-new actually replaced the posting, rather than just adding to it.
SELECT * FROM gin_dml_after_build_test WHERE tags % 'grape' ORDER BY id;

SELECT * FROM gin_dml_after_build_test ORDER BY id;

## cleanup
DROP TABLE IF EXISTS gin_dml_after_build_test;
