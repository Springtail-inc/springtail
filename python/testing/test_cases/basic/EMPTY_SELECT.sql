## test

-- Create table with primary key
CREATE TABLE IF NOT EXISTS test1 (
    id SERIAL PRIMARY KEY,
    username TEXT,
    email TEXT
);

-- Create secondary index
CREATE INDEX idx_test1_email ON test1 (email);

-- Create table with primary key only
CREATE TABLE IF NOT EXISTS test2 (
    id SERIAL PRIMARY KEY,
    name TEXT,
    department TEXT
);

CREATE TABLE IF NOT EXISTS test3 (
    name TEXT,
    age INT,
    city TEXT
);

-- Create table with no primary key
CREATE TABLE IF NOT EXISTS test4 (
    email TEXT,
    registered_at DATE,
    status TEXT
);

-- Create secondary index
CREATE INDEX idx_test4_status ON test4 (status);

## verify

-- SELECT using secondary index
SELECT * FROM test1 WHERE email = 'user@example.com';

-- SELECT using primary key
SELECT * FROM test2 WHERE id = 5;

-- SELECT using column (no index)
SELECT * FROM test3 WHERE city = 'Chicago';

-- SELECT using secondary index
SELECT * FROM test4 WHERE status = 'active';

## cleanup

DROP TABLE IF EXISTS test1 CASCADE;
DROP TABLE IF EXISTS test2 CASCADE;
DROP TABLE IF EXISTS test3 CASCADE;
DROP TABLE IF EXISTS test4 CASCADE;

