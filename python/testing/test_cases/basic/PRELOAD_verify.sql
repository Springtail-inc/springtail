## test

### restart

## verify

### schema_check data_type_test all_numeric_types
### schema_check data_type_test all_char_and_binary_types
### schema_check data_type_test all_datetime_types
-- ### schema_check data_type_test special_types_table
### schema_check data_type_test geometric_types_table
### schema_check data_type_test network_types_table
### schema_check data_type_test text_search_json
### schema_check data_type_test special_data
### schema_check data_type_test range_multirange_test

### schema_check keys_test table1
### schema_check keys_test table2
### schema_check keys_test table3
 
-- ### schema_check keys_test explicit_unique_1_index_test
-- ### schema_check keys_test explicit_unique_2_index_test
-- ### schema_check keys_test explicit_unique_1_null_index_test
-- ### schema_check keys_test explicit_unique_2_null_index_test
-- ### schema_check keys_test explicit_unique_1_partial_index_test
-- ### schema_check keys_test explicit_unique_2_partial_index_test
-- ### schema_check keys_test explicit_unique_1_func_index_test
-- ### schema_check keys_test explicit_unique_2_func_index_test

-- verify data types
SELECT * FROM data_type_test.all_numeric_types ORDER BY serial_col;
SELECT * FROM data_type_test.all_char_and_binary_types ORDER BY id;
SELECT * FROM data_type_test.all_datetime_types ORDER BY id;
-- SELECT * FROM data_type_test.special_types_table ORDER BY id;
SELECT * FROM data_type_test.geometric_types_table ORDER BY id;
SELECT * FROM data_type_test.network_types_table ORDER BY inet_col;
SELECT * FROM data_type_test.text_search_json ORDER BY id;
SELECT * FROM data_type_test.special_data ORDER BY id;
SELECT * FROM data_type_test.range_multirange_test ORDER BY id;


SELECT * FROM keys_test.table1 ORDER BY id;
SELECT * FROM keys_test.table1 ORDER BY col1;

SELECT * FROM keys_test.table2 ORDER BY id1;
SELECT * FROM keys_test.table2 ORDER BY id2;
SELECT * FROM keys_test.table2 ORDER BY id1, id2;
SELECT * FROM keys_test.table2 ORDER BY id2, id1;
SELECT * FROM keys_test.table2 ORDER BY col1;
SELECT * FROM keys_test.table2 ORDER BY col2;
SELECT * FROM keys_test.table2 ORDER BY col1, col2;
SELECT * FROM keys_test.table2 ORDER BY col2, col1;

SELECT * FROM keys_test.table3 ORDER BY id1;
SELECT * FROM keys_test.table3 ORDER BY id2;
SELECT * FROM keys_test.table3 ORDER BY id3;
SELECT * FROM keys_test.table3 ORDER BY id1, id2, id3;
SELECT * FROM keys_test.table3 ORDER BY id2, id3, id1;
SELECT * FROM keys_test.table3 ORDER BY id3, id1, id2;

-- this query should use the index
-- SELECT * FROM keys_test.explicit_unique_1_index_test ORDER BY username;
-- SELECT * FROM keys_test.explicit_unique_2_index_test ORDER BY username;
-- -- this query should use the index
-- SELECT * FROM keys_test.explicit_unique_2_index_test ORDER BY username, email;
-- SELECT * FROM keys_test.explicit_unique_2_index_test ORDER BY email, username;
-- SELECT * FROM keys_test.explicit_unique_2_index_test ORDER BY email;
-- 
-- -- this query should use the index
-- SELECT * FROM keys_test.explicit_unique_1_null_index_test ORDER BY username;
-- SELECT * FROM keys_test.explicit_unique_2_null_index_test ORDER BY username;
-- -- this query should use the index
-- SELECT * FROM keys_test.explicit_unique_2_null_index_test ORDER BY username, email;
-- SELECT * FROM keys_test.explicit_unique_2_null_index_test ORDER BY email, username;
-- SELECT * FROM keys_test.explicit_unique_2_null_index_test ORDER BY email;
-- 
-- SELECT * FROM keys_test.explicit_unique_1_partial_index_test ORDER BY username;
-- -- this query should use the index
-- SELECT * FROM keys_test.explicit_unique_1_partial_index_test ORDER BY username WHERE username IS NOT NULL;
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY username;
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY username WHERE username IS NOT NULL;
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY username, email;
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY username, email WHERE username IS NOT NULL;
-- -- this query should use the index
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY username, email WHERE username IS NOT NULL AND email IS NOT NULL;
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY email, username;
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY email, username WHERE username IS NOT NULL;
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY email, username WHERE username IS NOT NULL AND email IS NOT NULL;
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY email;
-- SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY email WHERE email IS NOT NULL;
-- 
-- -- this query should use the index
-- SELECT * FROM keys_test.explicit_unique_1_func_index_test ORDER BY username;
-- SELECT * FROM keys_test.explicit_unique_2_func_index_test ORDER BY username;
-- -- this query should use the index
-- SELECT * FROM keys_test.explicit_unique_2_func_index_test ORDER BY username, email;
-- SELECT * FROM keys_test.explicit_unique_2_func_index_test ORDER BY email, username;
-- SELECT * FROM keys_test.explicit_unique_2_func_index_test ORDER BY email;
-- 
-- 
-- 
-- 
-- 
