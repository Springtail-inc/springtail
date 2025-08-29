## test

CREATE TABLE partitions_alter_resync_multiple_parent (
    id   INT,
    name TEXT,
    role TEXT,
    family_name VARCHAR(100)
) PARTITION BY LIST (role);

CREATE TABLE partitions_alter_resync_multiple_child_001 (
    id   INT,
    name TEXT,
    role TEXT,
    family_name VARCHAR(100)
);

CREATE TABLE partitions_alter_resync_multiple_child_002 (
    id   INT,
    name TEXT,
    role TEXT,
    family_name VARCHAR(100)
);

CREATE TABLE partitions_alter_resync_multiple_child_003 (
    id   INT,
    name TEXT,
    role TEXT,
    family_name VARCHAR(100)
);

CREATE TABLE partitions_alter_resync_multiple_child_004 (
    id   INT,
    name TEXT,
    role TEXT,
    family_name VARCHAR(100)
);

CREATE TABLE partitions_alter_resync_multiple_child_005 (
    id   INT,
    name TEXT,
    role TEXT,
    family_name VARCHAR(100)
);


ALTER TABLE partitions_alter_resync_multiple_parent
    ATTACH PARTITION partitions_alter_resync_multiple_child_001 FOR VALUES IN ('Sibling');
ALTER TABLE partitions_alter_resync_multiple_parent
    ATTACH PARTITION partitions_alter_resync_multiple_child_002 FOR VALUES IN ('Nephew');
ALTER TABLE partitions_alter_resync_multiple_parent
    ATTACH PARTITION partitions_alter_resync_multiple_child_003 FOR VALUES IN ('Niece');
ALTER TABLE partitions_alter_resync_multiple_parent
    ATTACH PARTITION partitions_alter_resync_multiple_child_004 FOR VALUES IN ('Uncle');
ALTER TABLE partitions_alter_resync_multiple_parent
    ATTACH PARTITION partitions_alter_resync_multiple_child_005 FOR VALUES IN ('Aunt');

INSERT INTO partitions_alter_resync_multiple_parent VALUES (1, 'John', 'Sibling');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (2, 'Jane', 'Sibling');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (3, 'Bob', 'Uncle');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (4, 'Alice', 'Sibling');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (5, 'Bob', 'Aunt');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (6, 'Alice', 'Nephew');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (7, 'Bob', 'Niece');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (8, 'Alice', 'Nephew');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (9, 'Bob', 'Sibling');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (10, 'Alice', 'Sibling');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (11, 'Bob', 'Aunt');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (12, 'Alice', 'Nephew');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (13, 'Bob', 'Niece');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (14, 'Alice', 'Nephew');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (15, 'Bob', 'Sibling');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (16, 'Alice', 'Uncle');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (17, 'Bob', 'Sibling');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (18, 'Alice', 'Aunt');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (19, 'Bob', 'Niece');
INSERT INTO partitions_alter_resync_multiple_parent VALUES (20, 'Alice', 'Sibling');

ALTER TABLE partitions_alter_resync_multiple_parent
    DROP COLUMN family_name;

## verify
### schema_check public partitions_alter_resync_multiple_parent
### schema_check public partitions_alter_resync_multiple_child_001
### schema_check public partitions_alter_resync_multiple_child_002
### schema_check public partitions_alter_resync_multiple_child_003
### schema_check public partitions_alter_resync_multiple_child_004
### schema_check public partitions_alter_resync_multiple_child_005
SELECT * FROM partitions_alter_resync_multiple_parent ORDER BY id;

## cleanup
DROP TABLE partitions_alter_resync_multiple_parent CASCADE;
