## test
CREATE SEQUENCE IF NOT EXISTS seq_table_id_seq;

CREATE TABLE IF NOT EXISTS table_with_sequence (
    id INT NOT NULL DEFAULT nextval('seq_table_id_seq') PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

## verify
### schema_check public table_with_sequence

## cleanup
DROP TABLE IF EXISTS table_with_sequence;
DROP SEQUENCE IF EXISTS seq_table_id_seq;
