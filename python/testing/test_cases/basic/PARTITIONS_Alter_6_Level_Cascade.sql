## test
-- Root table
CREATE TABLE IF NOT EXISTS table1 (
    id SERIAL,
    part_key INT NOT NULL,
    data TEXT,
    PRIMARY KEY (id, part_key)
) PARTITION BY LIST (part_key);

-- Level 1 children
CREATE TABLE IF NOT EXISTS table2 PARTITION OF table1
    FOR VALUES IN (1) PARTITION BY LIST (part_key);

CREATE TABLE IF NOT EXISTS table3 PARTITION OF table1
    FOR VALUES IN (2) PARTITION BY LIST (part_key);

-- Level 2 children
CREATE TABLE IF NOT EXISTS table4 PARTITION OF table2
    FOR VALUES IN (10) PARTITION BY LIST (part_key);

CREATE TABLE IF NOT EXISTS table5 PARTITION OF table2
    FOR VALUES IN (11);

CREATE TABLE IF NOT EXISTS table6 PARTITION OF table3
    FOR VALUES IN (20) PARTITION BY LIST (part_key);

CREATE TABLE IF NOT EXISTS table7 PARTITION OF table3
    FOR VALUES IN (21);

-- Level 3 children
CREATE TABLE IF NOT EXISTS table8 PARTITION OF table4
    FOR VALUES IN (100) PARTITION BY LIST (part_key);

CREATE TABLE IF NOT EXISTS table9 PARTITION OF table6
    FOR VALUES IN (200) PARTITION BY LIST (part_key);

-- Level 4 children
CREATE TABLE IF NOT EXISTS table10 PARTITION OF table8
    FOR VALUES IN (1000);

CREATE TABLE IF NOT EXISTS table11 PARTITION OF table8
    FOR VALUES IN (1001);

CREATE TABLE IF NOT EXISTS table12 PARTITION OF table9
    FOR VALUES IN (2000);

ALTER TABLE table1 ADD COLUMN dummy TEXT;

## verify
### schema_check public table1
### schema_check public table2
### schema_check public table3
### schema_check public table4
### schema_check public table5
### schema_check public table6
### schema_check public table7
### schema_check public table8
### schema_check public table9
### schema_check public table10
### schema_check public table11
### schema_check public table12
SELECT * FROM table1 ORDER BY id;

## cleanup
DROP TABLE table1 CASCADE;
