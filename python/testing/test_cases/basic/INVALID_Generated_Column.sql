## test
-- Create a table with an invalid column
CREATE TABLE IF NOT EXISTS invalid_table_with_generated_column (
    id SERIAL PRIMARY KEY,
    length NUMERIC NOT NULL,
    width NUMERIC NOT NULL,
    area NUMERIC GENERATED ALWAYS AS (length * width) STORED
);

-- Alter the table to remove the invalid column
ALTER TABLE invalid_table_with_generated_column DROP COLUMN area;

## verify
SELECT * FROM invalid_table_with_generated_column;

## cleanup
DROP TABLE IF EXISTS invalid_table_with_generated_column;
