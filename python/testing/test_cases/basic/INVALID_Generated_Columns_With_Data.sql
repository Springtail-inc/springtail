## test
-- Create a table with an invalid column
CREATE TABLE IF NOT EXISTS invalid_table_with_generated_column_with_data (
    id SERIAL PRIMARY KEY,
    length NUMERIC NOT NULL,
    width NUMERIC NOT NULL,
    area NUMERIC GENERATED ALWAYS AS (length * width) STORED
);

INSERT INTO invalid_table_with_generated_column_with_data (length, width) VALUES (10, 20);
INSERT INTO invalid_table_with_generated_column_with_data (length, width) VALUES (30, 40);
INSERT INTO invalid_table_with_generated_column_with_data (length, width) VALUES (50, 60);

-- Alter the table to remove the invalid column
ALTER TABLE invalid_table_with_generated_column_with_data DROP COLUMN area;

ALTER TABLE invalid_table_with_generated_column_with_data ADD COLUMN area NUMERIC GENERATED ALWAYS AS (length * width) STORED;

## verify
SELECT 1;

## cleanup
DROP TABLE IF EXISTS invalid_table_with_generated_column_with_data;
