CREATE TABLE test_ingest (
  table_id INTEGER,
  name VARCHAR(50),
  offset INTEGER,
  PRIMARY KEY (table_id)
);

COPY persons(table_id,name,offset)
FROM './src/storage/test/test_btree_simple.csv'
DELIMITER ','
CSV HEADER;
