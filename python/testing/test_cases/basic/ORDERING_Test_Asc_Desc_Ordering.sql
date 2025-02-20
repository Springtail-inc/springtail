## test
CREATE TABLE asc_desc_ordering(
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
CREATE INDEX asc_desc_ordering_idx_col3 ON asc_desc_ordering (col3);
CREATE INDEX asc_desc_ordering_idx_col4_col5 ON asc_desc_ordering (col4, col5);
CREATE INDEX asc_desc_ordering_idx_col7 ON asc_desc_ordering (col7);

INSERT INTO asc_desc_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(1, 'Order A', '2025-02-05', 100.50, TRUE, 'First order description', 'Y', 10.5),
(2, 'Order B', '2025-02-06', 200.75, FALSE, 'Second order details', 'N', 20.2),
(3, 'Order C', '2025-02-07', 150.25, TRUE, 'Third order info', 'Y', 15.8);

INSERT INTO asc_desc_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(4, 'Order D', '2025-02-05', 99.99, FALSE, 'Fourth order notes', 'N', 9.9),
(5, 'Order E', '2025-02-06', 300.00, TRUE, 'Fifth order summary', 'Y', 30.0);

-- Insert focusing on col3 (indexed)
INSERT INTO asc_desc_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(6, 'Order F', '2025-02-05', 250.00, TRUE, 'Sixth order details', 'N', 25.5);

-- Insert focusing on col4, col5 (composite index)
INSERT INTO asc_desc_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(7, 'Order G', '2025-02-07', 500.00, FALSE, 'Seventh order details', 'Y', 50.5),
(8, 'Order H', '2025-02-08', 750.50, TRUE, 'Eighth order summary', 'N', 75.0);

-- Insert focusing on col7 (timestamp with default)
INSERT INTO asc_desc_ordering (col1, col2, col3, col4, col5, col6, col8, col9) 
VALUES 
(9, 'Order I', '2025-02-09', 120.00, FALSE, 'Ninth order info', 'Y', 12.0),
(10, 'Order J', '2025-02-10', 450.25, TRUE, 'Tenth order details', 'N', 45.2);

## verify
### schema_check public asc_desc_ordering
SELECT col3 FROM asc_desc_ordering ORDER BY col3 DESC;
SELECT col1, col2, col3 FROM asc_desc_ordering ORDER BY col4 ASC, col5 DESC;
SELECT col1, col2, col3 FROM asc_desc_ordering ORDER BY col6 ASC;

## cleanup
DROP TABLE IF EXISTS asc_desc_ordering CASCADE;