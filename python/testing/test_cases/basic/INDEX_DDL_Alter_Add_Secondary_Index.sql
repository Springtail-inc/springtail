## test
CREATE TABLE secondary_index_create(
    col1 INT NOT NULL,
    col2 VARCHAR(100) NOT NULL,
    col3 DATE,
    col4 DECIMAL(10, 2),
    col5 BOOLEAN,
    col6 TEXT,
    col7 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col8 CHAR(1),
    col9 FLOAT
);

-- Add Secondary Indexes
CREATE INDEX idx_col3 ON secondary_index_create (col3);
CREATE INDEX idx_col4_col5 ON secondary_index_create (col4, col5);
CREATE INDEX idx_col7 ON secondary_index_create (col7);

## verify
### schema_check public secondary_index_create

## cleanup
-- DROP TABLE IF EXISTS secondary_index_create;
SELECT 1;

