## test
-- GIN qual routing (btree redesign phase 12): PgFdwMgr must recognize pg_trgm's "%" similarity
-- operator against a READY gin_trgm_ops index and route it to a real GinReadStrategy lookup
-- (via a daemon-side GIN_EXTRACTVALUE RPC for the query-side trigram terms -- see
-- phases/phase-12.md's Design notes for why that RPC, not a direct opclass call, is required
-- here), instead of only the full-scan-plus-filter fallback GIN_Index_Build_And_Search.sql
-- (phase 11) exercises.
--
-- Builds on GIN_Index_Build_And_Search.sql's own setup exactly (same small, unsplit 3-row
-- table, same gin_trgm_ops opclass, same "insert before CREATE INDEX" ordering) -- see that
-- file's own comment for why pg_trgm, not btree_gin, is required in this environment, and why
-- the tree is deliberately kept tiny. (This file's own "insert before CREATE INDEX" choice is
-- kept for the same reason GIN_Index_Build_And_Search.sql keeps it -- GIN_DML_After_Build.sql
-- covers query routing composing correctly with rows that arrive/change/disappear *after* the
-- index is READY, closing what was a real gap here before btree redesign final-2.)
--
-- What this test can and can't assert, declaratively: plain SQL statements under "## verify"
-- below run against *both* the primary (real Postgres, real pg_trgm) and the replica (FDW) and
-- the two results are automatically compared (test_case.py's verify()) -- so the WHERE-"%"
-- queries below are a real correctness oracle: if GIN routing (or its OR-across-terms/recheck
-- design -- see phases/phase-12.md's Design notes) were wrong, these would fail by disagreeing
-- with real Postgres, not by disagreeing with a hand-maintained expected-rows list.
--
-- What this test does *not* assert: that the GIN path (rather than the full-scan/filter
-- fallback) was actually the one taken for these queries. There is no existing directive in
-- test_case.py for inspecting EXPLAIN plan content on the replica side distinctly from the
-- primary (the dual-compare verify() loop would just fail outright on an EXPLAIN, since a real
-- Postgres plan's text and Springtail's FDW plan's text never match; the "### benchmark"
-- directive runs EXPLAIN too but only ever extracts "Execution Time", not arbitrary plan
-- content) -- a real, named test-infrastructure gap, not a silent omission. This was instead
-- verified live during this phase's development, the same way phase 9/10 live-reproduced their
-- own fixes:
--   EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
--     SELECT * FROM <this table> WHERE tags % 'apple';
--   -- with routing enabled:  "Scan index: ...gin_trgm_ops_idx [GIN similarity]" / "Scan type: GIN index"
--   -- with _try_init_gin_scan() forced to return false: no such line, "Rows Removed by Filter: 1" instead
-- confirming the marker actually distinguishes the two paths. See phases/phase-12.md's Design
-- notes for the full transcript and a recommendation to add a plan-content-assertion directive
-- as separate, follow-on test-infrastructure work.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE gin_trgm_similarity_test (
    id INT PRIMARY KEY,
    tags TEXT
);

INSERT INTO gin_trgm_similarity_test (id, tags) VALUES
    (1, 'apple banana'), (2, 'banana cherry'), (3, 'apple cherry');

CREATE INDEX gin_trgm_similarity_test_idx ON gin_trgm_similarity_test USING gin (tags gin_trgm_ops);

## verify
### index_exists public gin_trgm_similarity_test gin_trgm_similarity_test_idx true

-- matches rows 1 and 3 (both share the "apple" trigram set) -- the GIN-routable case.
SELECT * FROM gin_trgm_similarity_test WHERE tags % 'apple' ORDER BY id;

-- matches no rows -- confirms an empty candidate set (or a term extraction that comes back
-- empty/no match) still returns correctly rather than erroring or over/under-matching.
SELECT * FROM gin_trgm_similarity_test WHERE tags % 'zzzzz' ORDER BY id;

-- same predicate, deliberately not pushable (the "(tags || '')" wrapper gives PgFdwMgr nothing
-- it recognizes as a GIN-eligible qual -- see _try_init_gin_scan()), so this always takes the
-- full-scan/filter fallback. Matching the first query's result set here is this test's own
-- "reference query that forces a full scan" the brief asks for, now checked at runtime against
-- real Postgres via the dual-compare mechanism rather than a hand-maintained row list.
SELECT * FROM gin_trgm_similarity_test WHERE (tags || '') % 'apple' ORDER BY id;

## cleanup
DROP TABLE IF EXISTS gin_trgm_similarity_test;
