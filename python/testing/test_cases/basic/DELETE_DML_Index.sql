## test
CREATE TABLE delete_dml_test (
    name TEXT,
    email TEXT,
    id SERIAL PRIMARY KEY,
    age INTEGER
);

INSERT INTO delete_dml_test (name, email, age) VALUES
    ('Alice', 'alice@example.com', 30),
    ('Bob', 'bob@example.com', 25),
    ('Charlie', 'charlie@example.com', 35),
    ('David', 'david@example.com', 40);

DELETE FROM delete_dml_test WHERE id = 3;

CREATE TABLE delete_dml_test_no_pk (
    name TEXT,
    email TEXT,
    age INTEGER
);

INSERT INTO delete_dml_test_no_pk (name, email, age) VALUES
    ('Ethan', 'ethan@example.com', 28),
    ('Fiona', 'fiona@example.com', 32),
    ('George', 'george@example.com', 45),
    ('Hannah', 'hannah@example.com', 38);

DELETE FROM delete_dml_test_no_pk WHERE age = 45;

CREATE TABLE delete_dml_test_compound (
    id INTEGER,
    email TEXT,
    age INTEGER,
    name TEXT,
    last_name TEXT,
    PRIMARY KEY (id, name)
);

INSERT INTO delete_dml_test_compound (id, email, name, last_name, age) VALUES
    (1, 'alice@example.com', 'Alice', 'Smith', 30),
    (2, 'bob@example.com', 'Bob', 'Johnson', 25),
    (3, 'charlie@example.com', 'Charlie', 'Williams', 35),
    (4, 'david@example.com', 'David', 'Jones', 40),
    (5, 'emily@example.com', 'Emily', 'Brown', 28);

DELETE FROM delete_dml_test_compound WHERE id = 2 AND name = 'Bob';
DELETE FROM delete_dml_test_compound WHERE name = 'David';

ALTER TABLE delete_dml_test_compound DROP COLUMN age;

INSERT INTO delete_dml_test_compound (id, email, name, last_name) VALUES
    (6, 'frank@example.com', 'Frank', 'Miller'),
    (7, 'grace@example.com', 'Grace', 'Davis');

DELETE FROM delete_dml_test_compound WHERE name = 'Emily';

## verify
SELECT * FROM delete_dml_test ORDER BY id;
SELECT * FROM delete_dml_test_no_pk ORDER BY name;
SELECT * FROM delete_dml_test_compound ORDER BY id;
SELECT * FROM delete_dml_test_compound ORDER BY name;

## cleanup
DROP TABLE IF EXISTS delete_dml_test;
DROP TABLE IF EXISTS delete_dml_test_no_pk;
DROP TABLE IF EXISTS delete_dml_test_compound;