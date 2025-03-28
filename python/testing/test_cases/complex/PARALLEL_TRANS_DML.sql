## metadata
### sync_timeout 30

## test
CREATE TABLE IF NOT EXISTS parallel_trans_dml (
    id SERIAL PRIMARY KEY,
    value TEXT
);

### parallel
### txn 1
INSERT INTO parallel_trans_dml (value)
SELECT 'Item-Trans-1-' || i
FROM generate_series(1, 5000) AS i;

### txn 2
INSERT INTO parallel_trans_dml (value)
SELECT 'Item-Trans-2-' || i
FROM generate_series(1, 5000) AS i;

## verify
SELECT * FROM parallel_trans_dml ORDER BY id;

## cleanup
DROP TABLE IF EXISTS parallel_trans_dml;
