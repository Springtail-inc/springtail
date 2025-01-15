## test
CREATE TABLE IF NOT EXISTS this_is_a_table_with_the_maximum_allowed_length_name_pgsql (
    id SERIAL PRIMARY KEY, 
    varchar_column VARCHAR(255) NOT NULL,
    int_column INT,
    boolean_column BOOLEAN,
    date_column DATE,
    timestamp_column TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    text_column TEXT
);

## verify
### schema_check public this_is_a_table_with_the_maximum_allowed_length_name_pgsql

## cleanup
DROP TABLE IF EXISTS this_is_a_table_with_the_maximum_allowed_length_name_pgsql;
