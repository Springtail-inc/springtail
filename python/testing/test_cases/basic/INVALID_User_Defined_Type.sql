## test
CREATE TYPE address_type AS (
    street TEXT,
    city TEXT,
    zip_code TEXT
);

-- Table using a composite type
CREATE TABLE IF NOT EXISTS invalid_table_with_user_defined_type (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    address address_type
);

-- Insert sample data
INSERT INTO invalid_table_with_user_defined_type (name, address)
VALUES ('John Doe', ROW('123 Main St', 'Springfield', '12345'));

-- Alter the table to remove the invalid column
ALTER TABLE invalid_table_with_user_defined_type DROP COLUMN address;

-- Wait for sync to complete
SELECT pg_sleep(3);

## verify
SELECT * FROM invalid_table_with_user_defined_type;

## cleanup
DROP TABLE IF EXISTS invalid_table_with_user_defined_type;
