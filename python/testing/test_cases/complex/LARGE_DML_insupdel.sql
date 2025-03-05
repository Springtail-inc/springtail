## test
-- Create tables with SERIAL primary keys
CREATE TABLE table1 (
    id SERIAL PRIMARY KEY,
    name TEXT,
    value INT
);

CREATE TABLE table2 (
    id SERIAL PRIMARY KEY,
    table1_id INT REFERENCES table1(id),
    description TEXT
);

CREATE TABLE table3 (
    id SERIAL PRIMARY KEY,
    table1_id INT REFERENCES table1(id),
    status TEXT
);

-- Insert 10,000 records into table1
INSERT INTO table1 (name, value)
SELECT 'Item-' || i, (RANDOM() * 1000)::INT
FROM generate_series(1, 10000) AS i;

-- Insert related records into table2
INSERT INTO table2 (table1_id, description)
SELECT id, 'Description for ' || id
FROM table1;

-- Insert related records into table3
INSERT INTO table3 (table1_id, status)
SELECT id, CASE WHEN id % 2 = 0 THEN 'Active' ELSE 'Inactive' END
FROM table1;

-- Perform bulk update on table1
UPDATE table1
SET value = value + 500
WHERE id IN (SELECT id FROM table1 ORDER BY RANDOM() LIMIT 5000);

-- Perform bulk update on table3
UPDATE table3
SET status = 'Updated'
WHERE table1_id IN (SELECT id FROM table1 ORDER BY RANDOM() LIMIT 3000);

-- Delete some records from table2 based on table1's value
DELETE FROM table2
WHERE table1_id IN (SELECT id FROM table1 WHERE value < 300 ORDER BY RANDOM() LIMIT 2000);

-- Delete some records from table3
DELETE FROM table3
WHERE table1_id IN (SELECT id FROM table1 ORDER BY RANDOM() LIMIT 1000);

## verify
SELECT
    (SELECT COUNT(*) FROM table1) AS count_table1,
    (SELECT COUNT(*) FROM table2) AS count_table2,
    (SELECT COUNT(*) FROM table3) AS count_table3;

## cleanup
DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
DROP TABLE IF EXISTS table3;
