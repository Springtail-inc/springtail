## test
-- Create a table without a primary key.
CREATE TABLE IF NOT EXISTS test_delete_duplicates (
    id SERIAL,
    value TEXT
);

-- Set replica identity for delete support.
ALTER TABLE test_delete_duplicates REPLICA IDENTITY FULL;

INSERT INTO test_delete_duplicates (value) VALUES ('duplicate');
INSERT INTO test_delete_duplicates (value) VALUES ('duplicate');

DELETE FROM test_delete_duplicates WHERE id = 1;

## verify
SELECT * FROM test_delete_duplicates ORDER BY id;

## cleanup
-- Cleanup
DROP TABLE IF EXISTS test_delete_duplicates;
