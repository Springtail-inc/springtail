## test
CREATE TABLE table_with_default_types_insert(
    serial_id SERIAL PRIMARY KEY,
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

INSERT INTO table_with_default_types_insert (col1, col2, col3, col4, col5, col6, col8, col9)
VALUES (1, 'Test String 1', '2025-01-01', 1234.56, TRUE, 'Sample text 1', 'A', 1.23);

INSERT INTO table_with_default_types_insert (col1, col2, col3, col4, col5, col6, col8, col9)
VALUES (2, 'Test String 2', '2025-02-15', 789.01, FALSE, 'Sample text 2', 'B', 4.56);

INSERT INTO table_with_default_types_insert (col1, col2, col3, col4, col5, col6, col8, col9)
VALUES (3, 'Test String 3', '2025-03-20', NULL, TRUE, 'Sample text 3', 'C', 7.89);

## verify
SELECT * FROM table_with_default_types_insert ORDER BY serial_id;

## cleanup
DROP TABLE IF EXISTS table_with_default_types_insert;
