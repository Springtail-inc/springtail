## test
CREATE TABLE IF NOT EXISTS parallel_trans_ddl (
    id SERIAL PRIMARY KEY,
    value TEXT
);

### parallel
### parallel
### txn 1
ALTER TABLE parallel_trans_ddl ADD COLUMN first_column TEXT;
INSERT INTO parallel_trans_ddl (first_column) VALUES ('First column value 1');
INSERT INTO parallel_trans_ddl (first_column) VALUES ('First column value 2');
ALTER TABLE parallel_trans_ddl ADD COLUMN third_column TEXT;
ALTER TABLE parallel_trans_ddl DROP COLUMN third_column;

### txn 2
ALTER TABLE parallel_trans_ddl ADD COLUMN second_column TEXT;
INSERT INTO parallel_trans_ddl (second_column) VALUES ('Second column value 1');
INSERT INTO parallel_trans_ddl (second_column) VALUES ('Second column value 2');
INSERT INTO parallel_trans_ddl (second_column) VALUES ('Second column value 3');
ALTER TABLE parallel_trans_ddl DROP COLUMN value;
INSERT INTO parallel_trans_ddl (second_column) VALUES ('Second column value 4');
ALTER TABLE parallel_trans_ddl DROP COLUMN second_column;

## verify
SELECT * FROM parallel_trans_ddl;

## cleanup
DROP TABLE IF EXISTS parallel_trans_ddl;
