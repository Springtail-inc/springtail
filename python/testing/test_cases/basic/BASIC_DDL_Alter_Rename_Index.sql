## test
-- Create a normal table with an index.
CREATE TABLE IF NOT EXISTS alter_idx_rename_test (
    a INT
);
CREATE INDEX IF NOT EXISTS alter_idx_rename_test_idx
    ON alter_idx_rename_test (a);

-- Create a  partitioned table with an index.
CREATE TABLE IF NOT EXISTS alter_idx_rename_test_parted (
    a INT
) PARTITION BY RANGE (a);
CREATE INDEX IF NOT EXISTS alter_idx_rename_test_parted_idx
    ON alter_idx_rename_test_parted (a);

-- Create partitions for different ranges
CREATE TABLE alter_idx_rename_test_parted_p1 PARTITION OF alter_idx_rename_test_parted
    FOR VALUES FROM (0) TO (10);

CREATE TABLE alter_idx_rename_test_parted_p2 PARTITION OF alter_idx_rename_test_parted
    FOR VALUES FROM (10) TO (100);

CREATE TABLE alter_idx_rename_test_parted_p3 PARTITION OF alter_idx_rename_test_parted
    FOR VALUES FROM (100) TO (1000);

-- TODO: Insert data to ensure the rename works with existing rows.
-- Insert different ranges of numbers
-- 30% small numbers (0-9)
-- 40% medium numbers (10-99)
-- 30% large numbers (100-999)
INSERT INTO alter_idx_rename_test (a)
SELECT CASE 
    WHEN random() < 0.3 THEN floor(random() * 10)::int
    WHEN random() < 0.7 THEN floor(random() * 90 + 10)::int
    ELSE floor(random() * 900 + 100)::int
END
FROM generate_series(1, 100);

INSERT INTO alter_idx_rename_test_parted (a)
SELECT CASE 
    WHEN random() < 0.3 THEN floor(random() * 10)::int
    WHEN random() < 0.7 THEN floor(random() * 90 + 10)::int
    ELSE floor(random() * 900 + 100)::int
END
FROM generate_series(1, 100);

-- Rename table with alter index statement
ALTER INDEX alter_idx_rename_test RENAME TO alter_idx_rename_test_2;
ALTER INDEX alter_idx_rename_test_parted RENAME TO alter_idx_rename_test_parted_2;

-- Rename index with alter index statement
ALTER INDEX alter_idx_rename_test_idx RENAME TO alter_idx_rename_test_idx_2;
ALTER INDEX alter_idx_rename_test_parted_idx RENAME TO alter_idx_rename_test_parted_idx_2;

-- Rename index with alter table statement
ALTER TABLE alter_idx_rename_test_idx_2 RENAME TO alter_idx_rename_test_idx_3;
ALTER TABLE alter_idx_rename_test_parted_idx_2 RENAME TO alter_idx_rename_test_parted_idx_3;

## verify
-- Query the renamed table to ensure the change is applied.
SELECT * FROM alter_idx_rename_test_2 ORDER BY a;
SELECT * FROM alter_idx_rename_test_parted_2 ORDER BY a;

## cleanup
-- Drop the renamed table to clean up.
DROP TABLE IF EXISTS alter_idx_rename_test_2 CASCADE;
DROP TABLE IF EXISTS alter_idx_rename_test_parted_2 CASCADE;
