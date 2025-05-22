## test

CREATE TABLE data_type_test.bit_table (
    id SERIAL PRIMARY KEY,
    bit_one_col BIT,
    bit_col BIT(8),
    varbit_col BIT VARYING
);

INSERT INTO data_type_test.bit_table (bit_one_col, bit_col, varbit_col)
VALUES
    (B'1', B'10101010', B'11001100'),
    (B'0', B'11110000', B'00110011'),
    (B'1', B'00001111', B'11111111');

### restart

## verify
-- data_type_test.basic_types_table (varchar_col, char_col, name_col)
SELECT varchar_col FROM data_type_test.basic_types_table ORDER BY id;
SELECT char_col FROM data_type_test.basic_types_table ORDER BY id;
SELECT name_col FROM data_type_test.basic_types_table ORDER BY id;
SELECT numeric_col FROM data_type_test.basic_types_table ORDER BY id;
SELECT * FROM data_type_test.basic_types_table ORDER BY id;

SELECT int_array_col FROM data_type_test.array_test_table ORDER BY id;
SELECT text_array_col FROM data_type_test.array_test_table ORDER BY id;
SELECT bool_array_col FROM data_type_test.array_test_table ORDER BY id;
SELECT * FROM data_type_test.array_test_table ORDER BY id;

SELECT mood_col FROM data_type_test.special_types_table ORDER BY id;
SELECT uuid_col FROM data_type_test.special_types_table ORDER BY id;
SELECT varbit_col FROM data_type_test.special_types_table ORDER BY id;
SELECT bit_col FROM data_type_test.special_types_table ORDER BY id;
SELECT * FROM data_type_test.special_types_table ORDER BY id;

SELECT bit_one_col FROM data_type_test.bit_table ORDER BY id;
SELECT bit_col FROM data_type_test.bit_table ORDER BY id;
SELECT varbit_col FROM data_type_test.bit_table ORDER BY id;
SELECT * FROM data_type_test.bit_table ORDER BY id;