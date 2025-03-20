## test
-- Create a table with an invalid column
CREATE TABLE IF NOT EXISTS invalid_table_with_non_standard_collation (
    id SERIAL PRIMARY KEY,
    name VARCHAR
);

INSERT INTO invalid_table_with_non_standard_collation (id, name) VALUES (1, 'First');
INSERT INTO invalid_table_with_non_standard_collation (id, name) VALUES (2, 'Second');

-- Alter the table to add an invalid column
ALTER TABLE invalid_table_with_non_standard_collation ADD COLUMN latin_text TEXT COLLATE "en-DE-x-icu";

INSERT INTO invalid_table_with_non_standard_collation (id, name, latin_text) VALUES (3, 'Third', 'Texte en français');

ALTER TABLE invalid_table_with_non_standard_collation DROP COLUMN latin_text;

## verify
-- Table shouldn't be present
SELECT * FROM invalid_table_with_non_standard_collation;

## cleanup
DROP TABLE IF EXISTS invalid_table_with_non_standard_collation;
