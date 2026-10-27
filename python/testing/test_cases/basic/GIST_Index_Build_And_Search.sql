## test
-- GiST strategy (btree redesign phase 10): a real "CREATE INDEX ... USING gist(...)" statement
-- must build and populate a real GiST tree end-to-end through Indexer/GistWriter, reaching
-- READY (not just carrying index_type/opclass metadata through to the catalog the way phase 9's
-- own INDEX_DDL_GiST_GIN_Access_Method.sql test proves for the dispatch-stub case).
--
-- Uses pg_trgm's gist_trgm_ops, not btree_gist's gist_text_ops, deliberately: confirmed directly
-- (not guessed) that btree_gist.so fails to dlopen in this environment at all (it references
-- dozens of Postgres builtin type-comparator functions -- bttextcmp, date_cmp, interval_cmp,
-- ... -- that only exist inside a real postgres backend process, not pg_ext's emulation layer),
-- matching why cube/hstore/pg_trgm, not btree_gist/btree_gin, are the extensions already
-- configured in system.json.settings' extension_config. pg_trgm must already be installed
-- before this daemon's one-time extension-registry load at startup sync (data_init.sql's own
-- "CREATE EXTENSION IF NOT EXISTS pg_trgm" -- see its comment for why the ordering matters).
--
-- Deliberately only 2 rows, inserted *before* CREATE INDEX (so Indexer's build-time full-table
-- scan populates both, rather than relying on post-build DML maintenance -- see phases/
-- phase-10.md's Design notes for why ongoing per-row DML maintenance for GiST indexes is an
-- explicitly flagged, deferred gap, not covered by this phase). Also deliberately small enough
-- to never trigger a split: this phase's own unit tests (test_gist_strategy.cc) found and
-- extensively documented a real, not-fully-root-caused crash in pg_trgm.so's own compiled
-- gtrgm_union()/gtrgm_penalty() under multi-entry/split conditions, confirmed independent of
-- several real pg_ext bugs fixed along the way (see phases/phase-10.md's Design notes) -- this
-- integration test exercises the real DDL-to-build pipeline for the confirmed-stable single/few-
-- row case; split/fan-out correctness is covered by test_gist_strategy.cc's own tests against a
-- pure-C++ reference opclass instead, specifically to avoid this flagged crash risk.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE gist_index_build_test (
    id INT PRIMARY KEY,
    val TEXT
);

INSERT INTO gist_index_build_test (id, val) VALUES (1, 'alpha'), (2, 'beta');

CREATE INDEX gist_index_build_test_idx ON gist_index_build_test USING gist (val gist_trgm_ops);

## verify
-- The index reaches the catalog and is built (not left at NOT_READY the way phase 9's dispatch-
-- stub test documents) -- __pg_springtail_catalog isn't queryable from a plain SQL line here
-- (this statement also runs, unmodified, against the primary's real Postgres, which has no such
-- schema), so index_exists (confirming the DDL replicated and the index object exists) plus the
-- table still answering ordinary queries correctly is this integration test's own level of
-- assertion; src/pg_log_mgr/test/test_indexer.cc's Indexer_Test.Test_GistBuildReachesReady
-- asserts the READY state transition directly.
### index_exists public gist_index_build_test gist_index_build_test_idx true

SELECT * FROM gist_index_build_test ORDER BY id;

## cleanup
DROP TABLE IF EXISTS gist_index_build_test;
