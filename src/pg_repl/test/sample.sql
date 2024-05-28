DROP TABLE IF EXISTS test_pgcopy;

CREATE TABLE test_pgcopy (
--  id SERIAL PRIMARY KEY,
  table_id INT,
  name VARCHAR(255) PRIMARY KEY,
  "offset" INT
);

\copy test_pgcopy(table_id, name, "offset") FROM '../../storage/test/test_btree_simple.csv' DELIMITER ',' CSV HEADER;
