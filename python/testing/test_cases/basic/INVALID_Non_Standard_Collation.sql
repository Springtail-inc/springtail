## test
-- Create a table with an invalid column
CREATE TABLE IF NOT EXISTS invalid_table_with_non_standard_collation (
    id SERIAL PRIMARY KEY,
    latin_text TEXT COLLATE "en-DE-x-icu"
);

-- Alter the table to remove the invalid column
ALTER TABLE invalid_table_with_non_standard_collation DROP COLUMN latin_text;

## verify
SELECT * FROM invalid_table_with_non_standard_collation;

## cleanup
DROP TABLE IF EXISTS invalid_table_with_non_standard_collation;
