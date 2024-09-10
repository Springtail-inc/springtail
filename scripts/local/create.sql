CREATE TABLE test_data (a INT PRIMARY KEY, b TEXT, c TIMESTAMP);
INSERT INTO test_data (a, b, c) VALUES (1, 'a', now());

CREATE TABLE test_data2 (a INTEGER[] PRIMARY KEY, b TEXT, c TIMESTAMP, d INTEGER);
INSERT INTO test_data2 (a, b, c, d) VALUES ('{1, 2}', 'a', now(), 1);

CREATE TABLE test_data3 (a INTEGER[], b TEXT PRIMARY KEY, c TIMESTAMP, d INTEGER);
INSERT INTO test_data3 (a, b, c, d) VALUES ('{1, 2}', 'a', now(), 1);

CREATE TABLE test_data4 (a INT PRIMARY KEY, b TEXT, c TIMESTAMP, d JSON);
INSERT INTO test_data4 (a, b, c, d) VALUES (1, 'a', now(), '{"bar": "baz", "balance": 7.77, "active": false}'::json);

CREATE TABLE test_data5 (a INT, b TEXT, c TIMESTAMP);
ALTER TABLE test_data5 REPLICA IDENTITY FULL;
INSERT INTO test_data5 (a, b, c) VALUES (1, 'a', now());
INSERT INTO test_data5 (a, b, c) VALUES (2, 'b', now());
