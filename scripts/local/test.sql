\set AUTOCOMMIT off

-- Test INSERT
BEGIN;
INSERT INTO test_data4 (a, b) VALUES (2, 'two');
INSERT INTO test_data4 (a, b, c) VALUES (3, 'three', now());
INSERT INTO test_data4 (a, b, c) VALUES (4, 'four', now());
INSERT INTO test_data4 (a, b) VALUES (5, 'five');
INSERT INTO test_data4 (a, b, d) VALUES (6, 'six', '{"bar": "baz", "balance": 7.77, "active": false}'::json);
COMMIT;

-- Test UPDATE
BEGIN;
UPDATE test_data4 SET b = 'one' WHERE a = 1;
COMMIT;

-- Test DELETE
BEGIN;
DELETE FROM test_data4 WHERE a = 5;
COMMIT;

-- Test UPDATE to a record that was INSERT in the same transaction
BEGIN;
INSERT INTO test_data4 (a, b, c) VALUES (5, 'fiv', now());
UPDATE test_data4 SET b = 'fivefive' WHERE a = 5;
COMMIT;

-- Test array types
BEGIN;
INSERT INTO test_data2 (a, b) VALUES ('{1, 3}', 'b');
INSERT INTO test_data2 (a, b) VALUES ('{1, 3, 5}', 'c');
INSERT INTO test_data2 (a, b) VALUES ('{1, 8, 5}', 'd');
COMMIT;

-- Test removing and adding a column within a single transaction
BEGIN;
INSERT INTO test_data3 (b, d) VALUES ('b', 14);
ALTER TABLE test_data3 DROP COLUMN d;
INSERT INTO test_data3 (b, c) VALUES ('c', now());
ALTER TABLE test_data3 ADD COLUMN e INTEGER;
INSERT INTO test_data3 (b, e) VALUES ('d', 15);
UPDATE test_data3 SET e = 14 WHERE b = 'b';
COMMIT;

-- Test adding and updating records to a table with no primary key
BEGIN;
INSERT INTO test_data5 (a, b, c) VALUES (4, 'd', now());
INSERT INTO test_data5 (a, b, c) VALUES (3, 'c', now());
UPDATE test_data5 SET b = 'A' WHERE a = 1;
COMMIT;

-- Test adding duplicate rows to a table with no primary key and then updating them
BEGIN;
INSERT INTO test_data5 (a, b, c) VALUES (5, 'e', now());
INSERT INTO test_data5 (a, b, c) VALUES (5, 'e', now());
UPDATE test_data5 SET b = 'E' WHERE a = 5;
COMMIT;
