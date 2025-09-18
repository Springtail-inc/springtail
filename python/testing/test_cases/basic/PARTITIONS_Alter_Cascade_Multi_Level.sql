## test
CREATE TABLE alter_cascade_level1 (
    id INT,
    name TEXT,
    role TEXT,
    subscription TEXT,
    region TEXT,
    dummy TEXT
) PARTITION BY LIST (subscription);

CREATE TABLE alter_cascade_level2 PARTITION OF alter_cascade_level1 FOR VALUES IN ('Paid')
PARTITION BY LIST (role);

CREATE TABLE alter_cascade_level2a PARTITION OF alter_cascade_level2 FOR VALUES IN ('Admin');
CREATE TABLE alter_cascade_level2b PARTITION OF alter_cascade_level2 FOR VALUES IN ('User');
CREATE TABLE alter_cascade_level2c PARTITION OF alter_cascade_level2 FOR VALUES IN ('Guest');

CREATE TABLE alter_cascade_level0 (
    id INT,
    name TEXT,
    role TEXT,
    subscription TEXT,
    region TEXT,
    dummy TEXT
) PARTITION BY LIST (region);

ALTER TABLE alter_cascade_level0 ATTACH PARTITION alter_cascade_level1 FOR VALUES IN ('US');

ALTER TABLE alter_cascade_level0 DROP COLUMN dummy;

## verify
### schema_check public alter_cascade_level0
### schema_check public alter_cascade_level1
### schema_check public alter_cascade_level2
### schema_check public alter_cascade_level2a
### schema_check public alter_cascade_level2b
### schema_check public alter_cascade_level2c
SELECT * FROM alter_cascade_level0 ORDER BY id;

## cleanup
DROP TABLE alter_cascade_level0 CASCADE;
