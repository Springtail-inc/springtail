-- Create test table
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Insert single row
INSERT INTO test_table (value) VALUES ('test_value');
