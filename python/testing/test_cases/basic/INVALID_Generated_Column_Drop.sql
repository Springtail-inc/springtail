## test
-- Create a table with an invalid column
CREATE TABLE IF NOT EXISTS invalid_table_with_generated_column_add_back_invalid (
    id SERIAL PRIMARY KEY,
    length NUMERIC NOT NULL,
    width NUMERIC NOT NULL,
    area NUMERIC GENERATED ALWAYS AS (length * width) STORED
);

INSERT INTO invalid_table_with_generated_column_add_back_invalid (length, width) VALUES (10, 20);
INSERT INTO invalid_table_with_generated_column_add_back_invalid (length, width) VALUES (30, 40);
INSERT INTO invalid_table_with_generated_column_add_back_invalid (length, width) VALUES (50, 60);

-- Alter the table to remove the invalid column
ALTER TABLE invalid_table_with_generated_column_add_back_invalid DROP COLUMN area;

-- Add back invalid column to ensure table is dropped
ALTER TABLE invalid_table_with_generated_column_add_back_invalid ADD COLUMN area NUMERIC GENERATED ALWAYS AS (length * width) STORED;

## verify
### table_exists public invalid_table_with_generated_column_add_back_invalid false

## cleanup
-- Nothing to do, as table should be dropped by the ALTER TABLE
