## test
CREATE TABLE nulls_ordering(
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
CREATE INDEX nulls_ordering_idx_col3 ON nulls_ordering (col3);
CREATE INDEX nulls_ordering_idx_col4_col5 ON nulls_ordering (col4, col5);
CREATE INDEX nulls_ordering_idx_col7 ON nulls_ordering (col7);

INSERT INTO nulls_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(1, 'Order A', '2025-02-05', 100.50, TRUE, 'First order description', 'Y', 10.5),
(2, 'Order B', NULL, 200.75, FALSE, 'Second order details', 'N', 20.2),
(3, 'Order C', '2025-02-07', NULL, TRUE, 'Third order info', 'Y', 15.8),
(4, 'Order D', '2025-02-05', 99.99, NULL, 'Fourth order notes', 'N', 9.9),
(5, 'Order E', NULL, NULL, NULL, 'Fifth order summary', 'Y', 30.0);

-- Insert focusing on col3 (indexed) with NULLs
INSERT INTO nulls_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(6, 'Order F', NULL, 250.00, TRUE, 'Sixth order details', 'N', 25.5);

-- Insert focusing on col4, col5 (composite index) with NULLs
INSERT INTO nulls_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(7, 'Order G', '2025-02-07', NULL, FALSE, 'Seventh order details', 'Y', 50.5),
(8, 'Order H', '2025-02-08', 750.50, NULL, 'Eighth order summary', 'N', 75.0);

-- Insert focusing on col7 (timestamp with default) with NULLs in other columns
INSERT INTO nulls_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(9, 'Order I', NULL, 120.00, FALSE, 'Ninth order info', 'Y', 12.0),
(10, 'Order J', '2025-02-10', 450.25, NULL, 'Tenth order details', 'N', 45.2),
(11, 'Order K', NULL, NULL, NULL, NULL, 'Y', NULL);

## verify
### schema_check public nulls_ordering
SELECT col3, col4 FROM nulls_ordering ORDER BY col3 ASC NULLS FIRST, col4 ASC NULLS FIRST;
SELECT col3, col4 FROM nulls_ordering ORDER BY col3 ASC NULLS LAST, col4 ASC NULLS LAST;

## cleanup
DROP TABLE IF EXISTS nulls_ordering;