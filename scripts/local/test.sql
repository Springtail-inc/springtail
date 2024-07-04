\set AUTOCOMMIT off
BEGIN;
INSERT INTO test_data4 (a, b) VALUES (2, 'two');
INSERT INTO test_data4 (a, b, c) VALUES (3, 'three', now());
INSERT INTO test_data4 (a, b, c) VALUES (4, 'four', now());
INSERT INTO test_data4 (a, b) VALUES (5, 'five');
INSERT INTO test_data4 (a, b, d) VALUES (6, 'six', '{"bar": "baz", "balance": 7.77, "active": false}'::json);
COMMIT;
BEGIN;
UPDATE test_data4 SET b = 'one' WHERE a = 1;
COMMIT;

BEGIN;
DELETE FROM test_data4 WHERE a = 5;
COMMIT;

BEGIN;
INSERT INTO test_data4 (a, b, c) VALUES (5, 'fiv', now());
UPDATE test_data4 SET b = 'fivefive' WHERE a = 5;
COMMIT;

BEGIN;
INSERT INTO test_data2 (a, b) VALUES ('{1, 3}', 'b');
INSERT INTO test_data2 (a, b) VALUES ('{1, 3, 5}', 'c');
INSERT INTO test_data2 (a, b) VALUES ('{1, 8, 5}', 'd');
COMMIT;
