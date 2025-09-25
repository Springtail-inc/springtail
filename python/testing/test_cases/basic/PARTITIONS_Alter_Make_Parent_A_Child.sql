## test
CREATE TABLE level1 (
    id INT,
    name TEXT,
    type TEXT,
    category TEXT,
    dummy TEXT
) PARTITION BY LIST (name);

CREATE TABLE level2 PARTITION OF level1 FOR VALUES IN ('Alice')
PARTITION BY LIST (type);

CREATE TABLE level2a PARTITION OF level2 FOR VALUES IN ('Type1');
CREATE TABLE level2b PARTITION OF level2 FOR VALUES IN ('Type2');

CREATE TABLE level0 (
    id INT,
    name TEXT,
    type TEXT,
    category TEXT,
    dummy TEXT
) PARTITION BY LIST (category);

ALTER TABLE level0 ATTACH PARTITION level1 FOR VALUES IN ('Category1');

INSERT INTO level0 VALUES (1, 'Alice', 'Type1', 'Category1', 'dummy');
INSERT INTO level0 VALUES (2, 'Alice', 'Type2', 'Category1', 'dummy');

## verify
### schema_check public level1
### schema_check public level2
### schema_check public level2a
### schema_check public level2b
### schema_check public level0
SELECT * FROM level0 ORDER BY id;

## cleanup
DROP TABLE level0 CASCADE;
