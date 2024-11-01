## test
-- Create a table without a primary key.
CREATE TABLE IF NOT EXISTS test_no_pkey (
    id SERIAL,
    value TEXT
);

-- Set replica identity for update support.
ALTER TABLE test_no_pkey REPLICA IDENTITY FULL;

INSERT INTO test_no_pkey (value) VALUES ('duplicate');
INSERT INTO test_no_pkey (value) VALUES ('duplicate');

UPDATE test_no_pkey SET value = 'updated' WHERE id = 1;

## verify
SELECT * FROM test_no_pkey;

## cleanup
-- Cleanup
DROP TABLE IF EXISTS test_no_pkey;
