## test
-- Index-kind (access-method) plumbing for GiST/GIN (btree redesign phase 9,
-- architecture.md Decisions log entry 8): a real "CREATE INDEX ... USING gist/gin" must
-- replicate end-to-end (proto, catalog) and reach the dispatch point, which must fail
-- explicitly ("not yet implemented") rather than silently building a wrong btree or crashing.
-- btree_gist/btree_gin let GiST/GIN index ordinary int/text columns without a geometry/array
-- extension (whose own pg_type OIDs aren't in Server::_check_index_columns' allowed-type list --
-- a separate, pre-existing restriction, unrelated to this phase).
--
-- Note: this test intentionally does not use "### schema_check" -- that directive waits,
-- database-wide (not scoped to any one table), for *every* secondary index to leave state 0
-- (NOT_READY). Neither index created below ever leaves state 0, so schema_check would time out
-- for the whole test run until "## cleanup" drops this table -- confirmed directly against a live
-- daemon stack (round-1 review of phase 11), not assumed from "phase 10/11 replaced the stub with
-- real builds":
--   - gist_gin_access_method_test_gist_idx (USING gist (val), val is INT, no explicit opclass):
--     GiST's own pre-existing TEXT-only scope limit (phase 10) rejects it --
--     "gist_helpers: only TEXT-typed GiST columns are supported in this phase".
--   - gist_gin_access_method_test_gin_idx (USING gin (tags), tags is TEXT, no explicit opclass):
--     Postgres resolves the implicit opclass to btree_gin's text_ops (confirmed against pg_opclass/
--     pg_index on the live primary), which is never registered with PgExtnRegistry because
--     btree_gin isn't in system.json.settings' extension_config (only cube/hstore/pg_trgm are) --
--     "gin_helpers: no GIN_EXTRACTVALUE registered for opclass text_ops".
-- Both failures are handled exactly as designed (a thrown Error, caught, index left NOT_READY
-- forever, no crash, no corruption) -- this is not a bug, but it does mean a bare
-- `CREATE INDEX ... USING gin(text_col)`/`USING gist(int_col)` with no explicit opclass silently
-- never becomes usable today, with no catalog-visible "failed" state. GIST_Index_Build_And_Search.
-- sql/GIN_Index_Build_And_Search.sql are the dedicated tests exercising the one opclass pair
-- (gist_trgm_ops/gin_trgm_ops) actually confirmed to reach READY. This test only ever asserted
-- index_exists (catalog presence), never state, so it remains correct regardless; schema_check
-- still isn't used here to avoid widening this file's own scope into re-asserting what those two
-- dedicated tests already cover. CREATE INDEX ... USING btree(...) regression coverage already
-- exists via INDEX_DDL_Alter_Add_Secondary_Index.sql's own ### schema_check, unaffected by
-- anything in this file.
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS btree_gin;

CREATE TABLE gist_gin_access_method_test (
    id INT PRIMARY KEY,
    val INT,
    tags TEXT
);

CREATE INDEX gist_gin_access_method_test_gist_idx ON gist_gin_access_method_test USING gist (val);
CREATE INDEX gist_gin_access_method_test_gin_idx ON gist_gin_access_method_test USING gin (tags);

INSERT INTO gist_gin_access_method_test (id, val, tags) VALUES (1, 10, 'alpha'), (2, 20, 'beta');

-- Inline CREATE TABLE secondary index: standard Postgres has no inline "INDEX ... USING
-- gist(...)" syntax; an EXCLUDE constraint is the one inline construct that creates a
-- non-primary GiST index as part of a single CREATE TABLE statement, so it's the real-world
-- shape of "the inline path" phase-9.md's checklist means.
--
-- This currently does NOT reach the catalog at all -- confirmed by direct repro (not guessed):
-- scripts/triggers.sql's inline-CREATE-TABLE event trigger loop (springtail_event_trigger_for_
-- table_ddl) has a genuine, pre-existing, independent bug -- swapped table_oid/index_identity
-- aliases (`c`, the index's own pg_class row, vs `ci`, the table's) -- that silently drops
-- replication of *every* inline-created secondary index (btree included, not just GiST/GIN;
-- confirmed by reproducing the identical failure with a plain inline UNIQUE constraint). See
-- phases/phase-9.md's "Deviations" section (Open Question 2) for the full writeup and exact
-- lines; it is out of this phase's scope to fix (this phase's own new index_type/opclass columns
-- are unaffected by it -- they attach to the correct alias regardless, confirmed directly against
-- the FDW Postgres cluster). The index_exists assertion below documents today's actual (broken)
-- behavior, so a future fix to the swap is forced to update this test rather than leave it
-- silently stale.
CREATE TABLE gist_inline_access_method_test (
    id INT PRIMARY KEY,
    val INT,
    EXCLUDE USING gist (val WITH =)
);

## verify
-- The GiST/GIN secondary indexes reach the catalog (proving the request carried index_type/
-- opclass correctly all the way from DDL replication through to catalog storage -- see
-- src/sys_tbl_mgr/test/test_sys_tbl_mgr.cc's CreateIndexAccessMethod unit test for the exact
-- index_type/opclass content assertions this integration test can't make directly --
-- __pg_springtail_catalog isn't queryable from a plain SQL line here, since this statement also
-- runs, unmodified, against the primary's real Postgres, which has no such schema). Both stay
-- NOT_READY forever (see the "## test" comment above for why); src/pg_log_mgr/test/test_indexer.cc's
-- Test_GistBuildReachesReady/Test_GinBuildReachesReady are the tests asserting a real READY
-- transition, using gist_trgm_ops/gin_trgm_ops explicitly rather than an implicit opclass.
### index_exists public gist_gin_access_method_test gist_gin_access_method_test_gist_idx true
### index_exists public gist_gin_access_method_test gist_gin_access_method_test_gin_idx true

-- The table itself, and ordinary queries against it, are unaffected by the still-building
-- (NOT_READY) GiST/GIN indexes -- the FDW simply doesn't use them yet (phase 12's job).
SELECT * FROM gist_gin_access_method_test ORDER BY id;

-- Known, pre-existing, independent bug (see the "## test" comment above): the inline EXCLUDE
-- constraint's index never reaches the catalog at all today.
### index_exists public gist_inline_access_method_test gist_inline_access_method_test_val_excl false

## cleanup
DROP TABLE IF EXISTS gist_gin_access_method_test;
DROP TABLE IF EXISTS gist_inline_access_method_test;
