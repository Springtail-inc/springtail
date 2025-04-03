## metadata
### sync_timeout 20

## test
CREATE TABLE IF NOT EXISTS parallel_trans_ddl_with_sleep (
    id SERIAL PRIMARY KEY,
    value TEXT
);

### parallel
### txn 1
ALTER TABLE parallel_trans_ddl_with_sleep ADD COLUMN first_column TEXT;
### sleep 1.5
INSERT INTO parallel_trans_ddl_with_sleep (first_column) VALUES ('First column value 1');
### sleep 1.5
INSERT INTO parallel_trans_ddl_with_sleep (first_column) VALUES ('First column value 2');
### sleep 1.5
ALTER TABLE parallel_trans_ddl_with_sleep ADD COLUMN third_column TEXT;
### sleep 1.5
ALTER TABLE parallel_trans_ddl_with_sleep DROP COLUMN third_column;

### txn 2
ALTER TABLE parallel_trans_ddl_with_sleep ADD COLUMN second_column TEXT;
### sleep 0.5
INSERT INTO parallel_trans_ddl_with_sleep (second_column) VALUES ('Second column value 1');
### sleep 0.5
INSERT INTO parallel_trans_ddl_with_sleep (second_column) VALUES ('Second column value 2');
### sleep 0.5
INSERT INTO parallel_trans_ddl_with_sleep (second_column) VALUES ('Second column value 3');
### sleep 0.5
ALTER TABLE parallel_trans_ddl_with_sleep DROP COLUMN value;
### sleep 0.5
INSERT INTO parallel_trans_ddl_with_sleep (second_column) VALUES ('Second column value 4');
### sleep 0.5
ALTER TABLE parallel_trans_ddl_with_sleep DROP COLUMN second_column;

## verify
SELECT * FROM parallel_trans_ddl_with_sleep;

## cleanup
DROP TABLE IF EXISTS parallel_trans_ddl_with_sleep;
