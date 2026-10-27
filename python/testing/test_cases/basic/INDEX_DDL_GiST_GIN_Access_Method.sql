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
-- (NOT_READY). The GiST/GIN indexes below are *never* going to leave state 0 under this phase's
-- dispatch stub (that's the point), so schema_check would time out for the whole test run until
-- "## cleanup" drops this table. CREATE INDEX ... USING btree(...) regression coverage already
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
-- src/sys_tbl_mgr/test/test_sys_tbl_mgr.cc's CreateIndexAccessMethod and
-- src/pg_log_mgr/test/test_indexer.cc's Test_GistGinDispatchNotImplemented unit tests for the
-- exact index_type/opclass/NOT_READY-state content assertions this integration test can't make
-- directly -- __pg_springtail_catalog isn't queryable from a plain SQL line here, since this
-- statement also runs, unmodified, against the primary's real Postgres, which has no such
-- schema).
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
