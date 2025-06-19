## test
-- Create a table with an invalid column
CREATE TABLE IF NOT EXISTS invalid_table_with_generated_column_with_dml (
    id SERIAL PRIMARY KEY,
    length NUMERIC NOT NULL,
    width NUMERIC NOT NULL,
    area NUMERIC GENERATED ALWAYS AS (length * width) STORED
);

INSERT INTO invalid_table_with_generated_column_with_dml (length, width) VALUES (10, 20);
INSERT INTO invalid_table_with_generated_column_with_dml (length, width) VALUES (30, 40);
INSERT INTO invalid_table_with_generated_column_with_dml (length, width) VALUES (50, 60);

UPDATE invalid_table_with_generated_column_with_dml SET length = 70, width = 80 WHERE id = 3;

DELETE FROM invalid_table_with_generated_column_with_dml WHERE length = 30 AND width = 40;

INSERT INTO invalid_table_with_generated_column_with_dml (length, width) VALUES (100, 200);

-- Alter the table to remove the invalid column
ALTER TABLE invalid_table_with_generated_column_with_dml DROP COLUMN area;

select pg_sleep(3);

## verify
SELECT * FROM invalid_table_with_generated_column_with_dml;

## cleanup
DROP TABLE IF EXISTS invalid_table_with_generated_column_with_dml;
