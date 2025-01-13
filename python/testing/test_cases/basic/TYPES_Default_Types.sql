## test
CREATE TABLE table_with_default_types(
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

## verify
### schema_check public table_with_default_types

## cleanup
DROP TABLE IF EXISTS table_with_default_types;
