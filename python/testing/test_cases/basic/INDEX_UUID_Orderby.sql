## test
CREATE TABLE uuid_table (
    id SERIAL PRIMARY KEY,
    uuid_column UUID
);

CREATE INDEX uuid_idx ON uuid_table (uuid_column);

-- Insert multiple rows 1000 rows
INSERT INTO uuid_table (uuid_column)
SELECT gen_random_uuid()
FROM generate_series(1, 1000);

## verify
SELECT * FROM uuid_table ORDER BY uuid_column ASC;
SELECT * FROM uuid_table ORDER BY uuid_column DESC;

## cleanup
DROP INDEX IF EXISTS uuid_idx;
DROP TABLE IF EXISTS uuid_table;


