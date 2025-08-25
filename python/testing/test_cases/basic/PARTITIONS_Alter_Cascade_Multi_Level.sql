## test
CREATE TABLE alter_cascade_level1 (
    id INT,
    name TEXT,
    type TEXT,
    category TEXT,
    dummy TEXT
) PARTITION BY LIST (name);

CREATE TABLE alter_cascade_level2 PARTITION OF alter_cascade_level1 FOR VALUES IN ('a')
PARTITION BY LIST (type);

CREATE TABLE alter_cascade_level2a PARTITION OF alter_cascade_level2 FOR VALUES IN ('a');
CREATE TABLE alter_cascade_level2b PARTITION OF alter_cascade_level2 FOR VALUES IN ('b');

CREATE TABLE alter_cascade_level0 (
    id INT,
    name TEXT,
    type TEXT,
    category TEXT,
    dummy TEXT
) PARTITION BY LIST (category);

ALTER TABLE alter_cascade_level0 ATTACH PARTITION alter_cascade_level1 FOR VALUES IN ('a');

ALTER TABLE alter_cascade_level0 DROP COLUMN dummy;

## verify
### schema_check public alter_cascade_level0
### schema_check public alter_cascade_level1
### schema_check public alter_cascade_level2
### schema_check public alter_cascade_level2a
### schema_check public alter_cascade_level2b
SELECT * FROM alter_cascade_level0 ORDER BY id;

## cleanup
DROP TABLE alter_cascade_level0 CASCADE;
