## test
CREATE TABLE family_parent_partition_drop (
    id   INT,
    name TEXT,
    role TEXT
) PARTITION BY LIST (role);

CREATE TABLE family_child_partition_drop PARTITION OF family_parent_partition_drop FOR VALUES IN ('Sibling');

INSERT INTO family_parent_partition_drop VALUES (1, 'John', 'Sibling');

DROP TABLE family_child_partition_drop;

## verify
### schema_check public family_parent_partition_drop
### table_exists public family_child_partition_drop false
SELECT * FROM family_parent_partition_drop ORDER BY id;

## cleanup
DROP TABLE family_parent_partition_drop CASCADE;
