## test
CREATE TABLE primary_index_create(
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

-- Add Composite Primary Key
ALTER TABLE primary_index_create 
ADD PRIMARY KEY (col1, col2);

## verify
### schema_check public primary_index_create

## cleanup
DROP TABLE IF EXISTS primary_index_create;
