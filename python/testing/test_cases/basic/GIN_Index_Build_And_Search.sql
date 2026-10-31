## test
-- GIN strategy (btree redesign phase 11): a real "CREATE INDEX ... USING gin(...)" statement
-- must build and populate a real GIN entry-tree/posting-list structure end-to-end through
-- Indexer/GinWriteStrategy, reaching READY (not just carrying index_type/opclass metadata through
-- to the catalog the way phase 9's own INDEX_DDL_GiST_GIN_Access_Method.sql test proves for the
-- dispatch-stub case).
--
-- Uses pg_trgm's gin_trgm_ops, not btree_gin's gin_text_ops: mirrors GIST_Index_Build_And_Search.sql's
-- identical choice for GiST, and confirmed directly (this phase's own test_gin_strategy.cc
-- RealOpclassManyRowsIsStable, 150 rows, 10 consecutive runs) that GIN_EXTRACTVALUE's real
-- gin_extract_value_trgm() is stable here -- unlike phase 10's flagged GIST union/penalty
-- instability. pg_trgm must already be installed before this daemon's one-time extension-registry
-- load at startup sync (data_init.sql's own "CREATE EXTENSION IF NOT EXISTS pg_trgm", shared with
-- the GiST test above).
--
-- Inserted *before* CREATE INDEX, so Indexer's build-time full-table scan is what populates these
-- rows -- kept this way for this test's own purpose (asserting the build itself reaches READY
-- and is populated), not because post-build DML maintenance is untested: that gap (phase 10/11
-- named it, no later phase closed it until btree redesign final-2) is covered by
-- GIN_DML_After_Build.sql instead, added alongside this file rather than changing this one's
-- already-asserted setup.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE gin_index_build_test (
    id INT PRIMARY KEY,
    tags TEXT
);

INSERT INTO gin_index_build_test (id, tags) VALUES
    (1, 'apple banana'), (2, 'banana cherry'), (3, 'apple cherry');

CREATE INDEX gin_index_build_test_idx ON gin_index_build_test USING gin (tags gin_trgm_ops);

## verify
-- The index reaches the catalog and is built (not left at NOT_READY the way phase 9's dispatch-
-- stub test documents) -- __pg_springtail_catalog isn't queryable from a plain SQL line here
-- (this statement also runs, unmodified, against the primary's real Postgres, which has no such
-- schema), so index_exists (confirming the DDL replicated and the index object exists) plus the
-- table still answering ordinary queries correctly is this integration test's own level of
-- assertion; src/pg_log_mgr/test/test_indexer.cc's Indexer_Test.Test_GinBuildReachesReady asserts
-- the READY state transition directly, and src/storage/test/test_gin_strategy.cc asserts real
-- posting-list intersection correctness directly against GinReadStrategy -- query-time routing of
-- a real Postgres qual into a GIN scan (so this result set would actually come back via the index
-- rather than a full scan) is explicitly phase 12's job, not this test's.
### index_exists public gin_index_build_test gin_index_build_test_idx true

SELECT * FROM gin_index_build_test ORDER BY id;

## cleanup
DROP TABLE IF EXISTS gin_index_build_test;
