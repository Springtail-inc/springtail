## test
-- Create a table with an invalid column
CREATE TABLE IF NOT EXISTS invalid_table_initially_valid_made_invalid (
    id SERIAL PRIMARY KEY,
    name VARCHAR
);

INSERT INTO invalid_table_initially_valid_made_invalid (id, name) VALUES (1, 'First');
INSERT INTO invalid_table_initially_valid_made_invalid (id, name) VALUES (2, 'Second');

-- Alter the table to add an invalid column
ALTER TABLE invalid_table_initially_valid_made_invalid ADD COLUMN latin_text TEXT COLLATE "en-DE-x-icu";

-- Insert data into the invalid column
INSERT INTO invalid_table_initially_valid_made_invalid (id, name, latin_text) VALUES (3, 'Third', 'Texte en français');

## verify
### verify_exists public invalid_table_initially_valid_made_invalid false

## cleanup
DROP TABLE IF EXISTS invalid_table_initially_valid_made_invalid;
