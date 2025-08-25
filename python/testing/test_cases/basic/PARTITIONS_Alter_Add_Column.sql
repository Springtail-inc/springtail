## test

CREATE TABLE family_alter_parent_partition (
   id   INT,
   name TEXT,
   role TEXT
) PARTITION BY LIST (role);

CREATE TABLE family_alter_child_partition_siblings (
   id   INT,
   name TEXT,
   role TEXT
);

ALTER TABLE family_alter_parent_partition
   ATTACH PARTITION family_alter_child_partition_siblings FOR VALUES IN ('Sibling');

INSERT INTO family_alter_parent_partition VALUES (1, 'John', 'Sibling');

ALTER TABLE family_alter_parent_partition
   ADD COLUMN family_name VARCHAR(100);

## verify
### schema_check public family_alter_parent_partition
### schema_check public family_alter_child_partition_siblings
SELECT * FROM family_alter_parent_partition ORDER BY id;

## cleanup
DROP TABLE family_alter_parent_partition CASCADE;
