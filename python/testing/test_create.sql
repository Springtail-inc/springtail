-- Create a schema to organize test tables
CREATE SCHEMA IF NOT EXISTS encoding_test;

-- Table with default encoding and collation
CREATE TABLE IF NOT EXISTS encoding_test.standard_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- Table with a column using a different collation (non-default encoding behavior)
CREATE TABLE IF NOT EXISTS encoding_test.collation_table (
    id SERIAL PRIMARY KEY,
    text_en TEXT COLLATE "en-US-x-icu",
    text_de TEXT COLLATE "de-DE-x-icu"
);

CREATE INDEX idx_text_en_text_de ON encoding_test.collation_table (text_en, text_de);

-- Table storing encoded data in BYTEA (e.g., for non-standard encoding like Shift_JIS)
CREATE TABLE IF NOT EXISTS encoding_test.binary_table (
    id SERIAL PRIMARY KEY,
    raw_data BYTEA NOT NULL
);

-- Table with mixed encoding behaviors (normal text + binary storage)
CREATE TABLE IF NOT EXISTS encoding_test.mixed_table (
    id SERIAL PRIMARY KEY,
    utf8_text TEXT NOT NULL,
    latin_text TEXT COLLATE "en-DE-x-icu",
    encoded_blob BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS encoding_test.lo_table (
    id SERIAL PRIMARY KEY,
    lo_text TEXT NOT NULL,
    lo_blob OID NOT NULL
);

-- Insert sample data
INSERT INTO encoding_test.standard_table (name) VALUES ('Normal Text');
INSERT INTO encoding_test.collation_table (text_en, text_de) VALUES ('Hello', 'Hallo');
INSERT INTO encoding_test.binary_table (raw_data) VALUES (decode('48656c6c6f', 'hex')); -- "Hello" in hex
INSERT INTO encoding_test.mixed_table (utf8_text, latin_text, encoded_blob)
VALUES ('UTF-8 Text', 'Texte en français', decode('e38386e382b9e38388', 'hex')); -- "テスト" (Test in Japanese Shift_JIS)

CREATE SCHEMA IF NOT EXISTS generated_test;

-- Table with a simple stored generated column
CREATE TABLE IF NOT EXISTS generated_test.area_table (
    id SERIAL PRIMARY KEY,
    length NUMERIC NOT NULL,
    width NUMERIC NOT NULL,
    area NUMERIC GENERATED ALWAYS AS (length * width) STORED
);

-- Table without any generated columns (for comparison)
CREATE TABLE IF NOT EXISTS generated_test.regular_table (
    id SERIAL PRIMARY KEY,
    description TEXT NOT NULL
);

-- Insert sample data
INSERT INTO generated_test.area_table (length, width) VALUES (10, 5);
INSERT INTO generated_test.regular_table (description) VALUES ('This is a normal table');

-- Create a schema for testing
CREATE SCHEMA IF NOT EXISTS user_defined_test;

-- Define a user-defined composite type
CREATE TYPE user_defined_test.address_type AS (
    street TEXT,
    city TEXT,
    zip_code TEXT
);

-- Define an ENUM type
CREATE TYPE user_defined_test.status_type AS ENUM ('active', 'inactive', 'pending');

-- Table using a composite type
CREATE TABLE IF NOT EXISTS user_defined_test.customer (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    address user_defined_test.address_type -- Uses a user-defined type
);

-- Table using an ENUM type
CREATE TABLE IF NOT EXISTS user_defined_test.orders (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    status user_defined_test.status_type NOT NULL -- Uses a user-defined type
);

-- Insert sample data
INSERT INTO user_defined_test.customer (name, address)
VALUES ('John Doe', ROW('123 Main St', 'Springfield', '12345'));

INSERT INTO user_defined_test.orders (customer_id, status)
VALUES (1, 'active');

CREATE SCHEMA IF NOT EXISTS json_test_1;

CREATE SCHEMA IF NOT EXISTS json_test_2;

CREATE SCHEMA IF NOT EXISTS json_test_3;

CREATE TABLE IF NOT EXISTS json_test_2.employee (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    salary NUMERIC NOT NULL
);

INSERT INTO json_test_2.employee (name, salary) VALUES ('John Doe', 50000);
INSERT INTO json_test_2.employee (name, salary) VALUES ('Jane Smith', 60000);
INSERT INTO json_test_2.employee (name, salary) VALUES ('Alice Johnson', 55000);

CREATE SCHEMA IF NOT EXISTS json_test_4;

CREATE TABLE IF NOT EXISTS json_test_4.invoice (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    amount NUMERIC NOT NULL,
    status TEXT NOT NULL
);

INSERT INTO json_test_4.invoice (customer_id, amount, status) VALUES (1, 1000, 'pending');
INSERT INTO json_test_4.invoice (customer_id, amount, status) VALUES (2, 2000, 'paid');
INSERT INTO json_test_4.invoice (customer_id, amount, status) VALUES (3, 3000, 'overdue');

CREATE TABLE IF NOT EXISTS json_test_4.invoice_item (
    id SERIAL PRIMARY KEY,
    invoice_id INT NOT NULL,
    item_name TEXT NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC NOT NULL
);

INSERT INTO json_test_4.invoice_item (invoice_id, item_name, quantity, price) VALUES (1, 'Item 1', 2, 500);
INSERT INTO json_test_4.invoice_item (invoice_id, item_name, quantity, price) VALUES (1, 'Item 2', 1, 300);
INSERT INTO json_test_4.invoice_item (invoice_id, item_name, quantity, price) VALUES (2, 'Item 3', 1, 1000);

CREATE SCHEMA IF NOT EXISTS json_test_5;

CREATE TABLE IF NOT EXISTS json_test_1.json_table (
    id SERIAL PRIMARY KEY,
    json_data JSONB NOT NULL
);
