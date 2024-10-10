# PHASE 1 -- table creation
CREATE TABLE test1 (colA text PRIMARY KEY, colB bigint);

# PHASE 2 -- data modification
INSERT INTO test1 (colA, colB) VALUES ('a', 1), ('b', 2), ('c', 3);
INSERT INTO test1 (colA, colB) VALUES ('d', 4);
UPDATE test1 SET colB = colB + 1 WHERE colA = 'a';

# PHASE 3 -- data verification
SELECT * FROM test1 ORDER BY colA;
SELECT colA FROM test1 ORDER BY colB;

# PHASE 4 -- data cleanup
DROP TABLE test1;
