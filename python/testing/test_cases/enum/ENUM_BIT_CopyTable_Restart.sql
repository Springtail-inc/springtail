## test

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
