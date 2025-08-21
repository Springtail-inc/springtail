## test

CREATE TABLE family_alter_parent_partition_drop (
    id   INT,
    name TEXT,
    role TEXT,
    family_name VARCHAR(100)
) PARTITION BY LIST (role);

CREATE TABLE family_alter_child_partition_drop_siblings (
    id   INT,
    name TEXT,
    role TEXT,
    family_name VARCHAR(100)
);

ALTER TABLE family_alter_parent_partition_drop
    ATTACH PARTITION family_alter_child_partition_drop_siblings FOR VALUES IN ('Sibling');

INSERT INTO family_alter_parent_partition_drop VALUES (1, 'John', 'Sibling');
INSERT INTO family_alter_parent_partition_drop VALUES (2, 'Jane', 'Sibling');
INSERT INTO family_alter_parent_partition_drop VALUES (3, 'Bob', 'Sibling');
INSERT INTO family_alter_parent_partition_drop VALUES (4, 'Alice', 'Sibling');

ALTER TABLE family_alter_parent_partition_drop
    DROP COLUMN family_name;

-- Wait for sync to complete
SELECT pg_sleep(10);

## verify
### schema_check public family_alter_parent_partition_drop
### schema_check public family_alter_child_partition_drop_siblings
SELECT * FROM family_alter_parent_partition_drop ORDER BY id;

## cleanup
DROP TABLE family_alter_parent_partition_drop CASCADE;
