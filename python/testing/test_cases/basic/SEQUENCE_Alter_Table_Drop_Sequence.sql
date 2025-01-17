## test
CREATE SEQUENCE IF NOT EXISTS seq_to_be_removed_after_alter;

CREATE TABLE IF NOT EXISTS table_with_sequence_to_be_droppped (
    id INT NOT NULL DEFAULT nextval('seq_to_be_removed_after_alter') PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE table_with_sequence_to_be_droppped ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS seq_to_be_removed_after_alter;

## verify
### schema_check public table_with_sequence_to_be_droppped

## cleanup
DROP TABLE IF EXISTS table_with_sequence_to_be_droppped;

