## test

-- table with a primary key
CREATE TABLE IF NOT EXISTS sample_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER CHECK (age >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO sample_table (name, email, age) VALUES
    ('Diana', 'diana@example.com', 28),
    ('Ethan', 'ethan@example.com', 35),
    ('Fiona', 'fiona@example.com', 22),
    ('George', 'george@example.com', 45),
    ('Hannah', 'hannah@example.com', 31),
    ('Ian', 'ian@example.com', 27),
    ('Jane', 'jane@example.com', 33),
    ('Kevin', 'kevin@example.com', 29),
    ('Laura', 'laura@example.com', 24),
    ('Mike', 'mike@example.com', 38);

INSERT INTO sample_table (name, email, age) VALUES
    ('Alice',   'alice@example.com',         28),
    ('Bob',     'bob@workplace.org',         34),
    ('Carol',   'carol@gmail.com',           22),
    ('Dave',    'dave123@yahoo.com',         29),
    ('Eve',     'eve@university.edu',        31),
    ('Frank',   'frank-the-tank@outlook.com',24),
    ('Grace',   'grace@company.net',         27),
    ('Heidi',   'heidi@research.lab',        35),
    ('Ivan',    'ivan1990@mail.com',         40),
    ('Judy',    'judy@startup.io',           26),
    ('Karl',    'karl+newsletter@gmail.com', 30),
    ('Lena',    'lena@hospital.org',         33),
    ('Mark',    'mark@a.b.c',                36),
    ('Nina',    'nina@weird-domain.xyz',     38),
    ('Oscar',   'oscar@anotherexample.com',  25),
    ('Peggy',   'peggy@secure.mail',         29),
    ('Quincy',  'quincy@my-custom.co',       32),
    ('Ruth',    'ruth@government.gov',       45),
    ('Steve',   'steve@edu.institute',       41),
    ('Trudy',   'trudy@dev.null',            39),
    ('Charlie', 'charlie@example.com',       45);

-- Updates and deletes using primary key

-- Update name and age for user with id = 2
UPDATE sample_table SET name = 'Ethan Updated', age = 36 WHERE id = 2;

-- Update email for user with id = 5
UPDATE sample_table SET email = 'hannah.new@example.com' WHERE id = 5;

-- Delete the user with id = 4
DELETE FROM sample_table WHERE id = 4;

-- Delete the user with id = 9
DELETE FROM sample_table WHERE id = 9;

-- Updates and deletes without using primary key

-- Update age where name is 'Alice'
UPDATE sample_table SET age = 29 WHERE name = 'Alice';

-- Update email where age is 40
UPDATE sample_table SET email = 'updated_email@example.com' WHERE age = 40;

-- Update name where email matches exactly
UPDATE sample_table SET name = 'Charlie Renamed' WHERE email = 'charlie@example.com';

-- Increment age by 1 for everyone aged over 30
UPDATE sample_table SET age = age + 1 WHERE age > 30;

-- Delete all rows where age is less than 25
DELETE FROM sample_table WHERE age < 25;

-- Delete rows where email domain is '@example.com'
DELETE FROM sample_table WHERE email LIKE '%@example.com';

-- Delete user named 'Grace'
DELETE FROM sample_table WHERE name = 'Grace';

-- Delete users with age between 35 and 40 (inclusive)
DELETE FROM sample_table WHERE age BETWEEN 35 AND 40;

-- table without primary key
CREATE TABLE anon_table (
    id          INTEGER,
    department  TEXT,
    role        TEXT,
    location    TEXT,
    salary      NUMERIC
);

INSERT INTO anon_table (id, department, role, location, salary) VALUES
    (101, 'Engineering', 'Developer',    'New York',    90000),
    (102, 'Engineering', 'Developer',    'New York',    92000),
    (103, 'Engineering', 'Tester',       'Boston',      70000),
    (104, 'HR',          'Recruiter',    'Chicago',     60000),
    (105, 'HR',          'Manager',      'Chicago',     85000),
    (106, 'Finance',     'Analyst',      'New York',    75000),
    (107, 'Finance',     'Manager',      'Boston',      95000),
    (108, 'IT',          'Support',      'New York',    65000),
    (109, 'IT',          'Support',      'Boston',      66000),
    (110, 'IT',          'Admin',        'Chicago',     68000),
    (111, 'Marketing',   'Executive',    'Chicago',     72000),
    (112, 'Marketing',   'Designer',     'New York',    71000),
    (113, 'Marketing',   'Executive',    'Boston',      73000),
    (114, 'Legal',       'Advisor',      'Boston',      88000),
    (115, 'Legal',       'Advisor',      'New York',    87000);

INSERT INTO anon_table (id, department, role, location, salary) VALUES
    (116, 'Engineering', 'Developer',    'New York',    91000),
    (117, 'Engineering', 'Developer',    'New York',    90500),
    (118, 'Engineering', 'Developer',    'Boston',      87000),
    (119, 'Engineering', 'Tester',       'New York',    72000),
    (120, 'Engineering', 'Tester',       'Boston',      71000),
    (121, 'Engineering', 'Manager',      'New York',    105000),
    (122, 'Engineering', 'Manager',      'Chicago',     102000),
    (123, 'Engineering', 'Architect',    'Boston',      110000),
    (124, 'Engineering', 'Intern',       'New York',    50000),
    (125, 'Engineering', 'Intern',       'Chicago',     48000);

UPDATE anon_table SET salary = salary + 5000 WHERE location = 'New York';
DELETE FROM anon_table WHERE salary < 70000;
UPDATE anon_table SET location = 'Remote' WHERE department = 'Engineering';

-- Boost all Engineering Developers in New York
UPDATE anon_table SET salary = salary + 2000
    WHERE department = 'Engineering' AND role = 'Developer' AND location = 'New York';

-- Remove low-paid Engineering Interns
DELETE FROM anon_table WHERE department = 'Engineering' AND role = 'Intern' AND salary < 49000;

## verify

SELECT * FROM sample_table ORDER BY id;
SELECT * FROM anon_table ORDER BY id;

## cleanup

DROP TABLE IF EXISTS sample_table;
DROP TABLE IF EXISTS anon_table;
