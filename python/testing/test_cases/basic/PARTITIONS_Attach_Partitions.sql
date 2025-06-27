## test

CREATE TABLE family_attach_partition (
    id   INT,
    name TEXT,
    role TEXT
) PARTITION BY LIST (role);

CREATE TABLE family_attach_partition_siblings (
    id   INT,
    name TEXT,
    role TEXT
);

CREATE TABLE family_attach_partition_cousins (
    id   INT,
    name TEXT,
    role TEXT
);

INSERT INTO family_attach_partition_siblings VALUES (1, 'John', 'Sibling');
INSERT INTO family_attach_partition_siblings VALUES (2, 'Jane', 'Sibling');
INSERT INTO family_attach_partition_siblings VALUES (3, 'Bob', 'Sibling');

INSERT INTO family_attach_partition_cousins VALUES (4, 'Mary', 'Cousin');
INSERT INTO family_attach_partition_cousins VALUES (5, 'Tom', 'Cousin');
INSERT INTO family_attach_partition_cousins VALUES (6, 'Jerry', 'Cousin');

ALTER TABLE family_attach_partition
    ATTACH PARTITION family_attach_partition_siblings FOR VALUES IN ('Sibling');

ALTER TABLE family_attach_partition
    ATTACH PARTITION family_attach_partition_cousins FOR VALUES IN ('Cousin');

## verify
### schema_check public family_attach_partition
### schema_check public family_attach_partition_siblings
### schema_check public family_attach_partition_cousins
SELECT * FROM family_attach_partition ORDER BY id;

## cleanup
DROP TABLE family_attach_partition CASCADE;
