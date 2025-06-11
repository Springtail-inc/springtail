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

ALTER TABLE family_attach_partition
    ATTACH PARTITION family_attach_partition_siblings FOR VALUES IN ('Sibling');

INSERT INTO family_attach_partition VALUES (1, 'John', 'Sibling');

## verify
SELECT * FROM family_attach_partition;

## cleanup
DROP TABLE family_attach_partition CASCADE;
