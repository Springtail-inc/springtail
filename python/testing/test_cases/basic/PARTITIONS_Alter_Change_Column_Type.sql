## test

CREATE TABLE family_alter_type_parent_partition (
    id   INT,
    name TEXT,
    role TEXT,
    area_code INT
) PARTITION BY LIST (role);

CREATE TABLE family_alter_type_child_partition_siblings (
    id   INT,
    name TEXT,
    role TEXT,
    area_code INT
);

ALTER TABLE family_alter_type_parent_partition
    ATTACH PARTITION family_alter_type_child_partition_siblings FOR VALUES IN ('Sibling');

INSERT INTO family_alter_type_parent_partition VALUES (1, 'John', 'Sibling', 123);

ALTER TABLE family_alter_type_parent_partition
    ALTER COLUMN area_code TYPE BIGINT;

INSERT INTO family_alter_type_parent_partition VALUES (2, 'Jane', 'Sibling', 1001);

## verify
### schema_check public family_alter_type_parent_partition
### schema_check public family_alter_type_child_partition_siblings
SELECT * FROM family_alter_type_parent_partition ORDER BY id;

## cleanup
DROP TABLE family_alter_type_parent_partition CASCADE;
