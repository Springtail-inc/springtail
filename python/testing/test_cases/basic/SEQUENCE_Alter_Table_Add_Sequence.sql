## test
CREATE TABLE IF NOT EXISTS table_without_sequence (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE SEQUENCE IF NOT EXISTS seq_table_id_seq;

ALTER TABLE table_without_sequence ALTER COLUMN id SET DEFAULT nextval('seq_table_id_seq');

## verify
### schema_check public table_without_sequence

## cleanup
DROP TABLE IF EXISTS table_without_sequence;
DROP SEQUENCE IF EXISTS seq_table_id_seq;

