DROP TABLE IF EXISTS test_ingest;

CREATE TABLE test_ingest (
  id SERIAL PRIMARY KEY,
  table_id INT,
  name VARCHAR(255),
  "offset" INT
);

\copy test_ingest(table_id, name, "offset") FROM '../../storage/test/test_btree_simple.csv' DELIMITER ',' CSV HEADER;
