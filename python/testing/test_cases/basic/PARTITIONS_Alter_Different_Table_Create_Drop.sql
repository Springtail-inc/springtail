## test
CREATE TABLE alter_txn_cascade_level1 (
    id INT,
    name TEXT,
    type TEXT,
    category TEXT,
    dummy TEXT
) PARTITION BY LIST (name);

CREATE TABLE alter_txn_cascade_level2 PARTITION OF alter_txn_cascade_level1 FOR VALUES IN ('a')
PARTITION BY LIST (type);

CREATE TABLE alter_txn_cascade_level0 (
    id INT,
    name TEXT,
    type TEXT,
    category TEXT,
    dummy TEXT
) PARTITION BY LIST (category);

ALTER TABLE alter_txn_cascade_level0 ATTACH PARTITION alter_txn_cascade_level1 FOR VALUES IN ('a');

BEGIN;
ALTER TABLE alter_txn_cascade_level0 DROP COLUMN dummy;

CREATE TABLE dummy_0 (
    id INT,
    name TEXT,
    type TEXT,
    category TEXT,
    dummy TEXT
);
DROP TABLE dummy_0;
COMMIT;

## verify
### schema_check public alter_txn_cascade_level0
### schema_check public alter_txn_cascade_level1
### schema_check public alter_txn_cascade_level2
### table_exists public dummy_0 false
SELECT * FROM alter_txn_cascade_level0 ORDER BY id;

## cleanup
DROP TABLE alter_txn_cascade_level0 CASCADE;
