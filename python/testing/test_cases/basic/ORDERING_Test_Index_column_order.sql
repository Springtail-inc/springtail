## metadata
### autocommit false

## test
BEGIN;
CREATE TABLE index_col_ordering(
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
COMMIT;

BEGIN;
INSERT INTO index_col_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(1, 'value_3', '2025-02-09', 120.00, FALSE, 'value_90', 'Y', 12.0),
(2, 'value_90', '2025-02-10', 450.25, TRUE, 'value_91', 'N', 45.2);
COMMIT;

-- Add Secondary Index with different column order than the table
BEGIN;
CREATE INDEX index_col_ordering_idx_col6_col2 ON index_col_ordering (col6, col2);
COMMIT;

-- Allow index reconciliation to be completed
SELECT pg_sleep(1);

BEGIN;
INSERT INTO index_col_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(1, 'value_3', '2025-02-09', 120.00, FALSE, 'value_90', 'Y', 12.0),
(2, 'value_90', '2025-02-10', 450.25, TRUE, 'value_91', 'N', 45.2);
COMMIT;

## verify
### schema_check public index_col_ordering
SELECT col1, col2, col3 FROM index_col_ordering ORDER BY col6 ASC, col2 ASC;

## cleanup
BEGIN;
DROP TABLE IF EXISTS index_col_ordering CASCADE;
COMMIT;
