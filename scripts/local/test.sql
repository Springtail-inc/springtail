\set AUTOCOMMIT off
BEGIN;
INSERT INTO test_data (a, b) VALUES (2, 'two');
INSERT INTO test_data (a, b) VALUES (3, 'three');
INSERT INTO test_data (a, b) VALUES (4, 'four');
INSERT INTO test_data (a, b) VALUES (5, 'five');
INSERT INTO test_data (a, b) VALUES (6, 'six');
COMMIT;
BEGIN;
UPDATE test_data SET b = 'one' WHERE a = 1;
COMMIT;

BEGIN;
DELETE FROM test_data WHERE a = 5;
COMMIT;

BEGIN;
INSERT INTO test_data (a, b) VALUES (5, 'fiv');
UPDATE test_data SET b = 'fivefive' WHERE a = 5;
COMMIT;
