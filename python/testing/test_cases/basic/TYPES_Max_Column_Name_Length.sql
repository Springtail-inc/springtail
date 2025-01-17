## test
CREATE TABLE IF NOT EXISTS table_with_max_column_name_length (
    id SERIAL PRIMARY KEY, 
    this_is_a_column_with_maximum_allowed_length_name_001 VARCHAR(255),
    this_is_a_column_with_maximum_allowed_length_name_002 INT,
    this_is_a_column_with_maximum_allowed_length_name_003 BOOLEAN,
    this_is_a_column_with_maximum_allowed_length_name_004 DATE,
    this_is_a_column_with_maximum_allowed_length_name_005 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    this_is_a_column_with_maximum_allowed_length_name_006 TEXT,
    this_is_a_column_with_maximum_allowed_length_name_007 FLOAT,
    this_is_a_column_with_maximum_allowed_length_name_008 MONEY
);

## verify
### schema_check public table_with_max_column_name_length

## cleanup
DROP TABLE IF EXISTS table_with_max_column_name_length;
