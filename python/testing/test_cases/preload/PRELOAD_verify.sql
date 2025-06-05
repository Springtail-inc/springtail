## test

### restart

## verify

### switch_db data_types
### schema_check data_type_test all_numeric_types
### schema_check data_type_test all_char_and_binary_types
### schema_check data_type_test all_datetime_types
### schema_check data_type_test special_types_table
### schema_check data_type_test geometric_types_table
### schema_check data_type_test network_types_table
### schema_check data_type_test text_search_json
### table_exists data_type_test composite_data false
### schema_check data_type_test special_data
### schema_check data_type_test range_multirange_test

### switch_db indices
### schema_check keys_test table1
### schema_check keys_test table2
### schema_check keys_test table3
 
### schema_check keys_test explicit_unique_1_index_test
### schema_check keys_test explicit_unique_2_index_test
### schema_check keys_test explicit_unique_1_null_index_test
### schema_check keys_test explicit_unique_2_null_index_test
### schema_check keys_test explicit_unique_1_partial_index_test
### schema_check keys_test explicit_unique_2_partial_index_test
### index_exists keys_test explicit_unique_2_partial_index_test explicit_unique_2_partial_index_test_idx true
### index_exists keys_test explicit_unique_1_func_index_test explicit_unique_1_func_index_test_idx false
### index_exists keys_test explicit_unique_2_func_index_test explicit_unique_2_func_index_test_idx false
### table_exists keys_test explicit_unique_1_func_index_test true
### table_exists keys_test explicit_unique_2_func_index_test true
### schema_check keys_test employees

-- verify data types
### switch_db data_types
SELECT * FROM data_type_test.all_numeric_types ORDER BY serial_col;
SELECT * FROM data_type_test.all_char_and_binary_types ORDER BY id;
SELECT * FROM data_type_test.all_datetime_types ORDER BY id;
SELECT * FROM data_type_test.special_types_table ORDER BY id;
SELECT * FROM data_type_test.geometric_types_table ORDER BY id;
SELECT * FROM data_type_test.network_types_table ORDER BY inet_col;
SELECT * FROM data_type_test.text_search_json ORDER BY id;
SELECT * FROM data_type_test.special_data ORDER BY id;
SELECT * FROM data_type_test.range_multirange_test ORDER BY id;

-- verify indices
### switch_db indices
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
SELECT * FROM keys_test.explicit_unique_1_index_test ORDER BY username;
SELECT * FROM keys_test.explicit_unique_2_index_test ORDER BY username;
-- this query should use the index
SELECT * FROM keys_test.explicit_unique_2_index_test ORDER BY username, email;
SELECT * FROM keys_test.explicit_unique_2_index_test ORDER BY email, username;
SELECT * FROM keys_test.explicit_unique_2_index_test ORDER BY email;

-- this query should use the index
SELECT * FROM keys_test.explicit_unique_1_null_index_test ORDER BY username, email;
-- this query should use the index
SELECT * FROM keys_test.explicit_unique_2_null_index_test ORDER BY username, email;
SELECT * FROM keys_test.explicit_unique_2_null_index_test ORDER BY email, username;

SELECT * FROM keys_test.explicit_unique_1_partial_index_test ORDER BY username;
-- this query should use the index
SELECT * FROM keys_test.explicit_unique_1_partial_index_test WHERE username IS NOT NULL ORDER BY username;
SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY username;
SELECT * FROM keys_test.explicit_unique_2_partial_index_test WHERE username IS NOT NULL ORDER BY username;
SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY username, email;
SELECT * FROM keys_test.explicit_unique_2_partial_index_test WHERE username IS NOT NULL ORDER BY username, email;
-- this query should use the index
SELECT * FROM keys_test.explicit_unique_2_partial_index_test WHERE username IS NOT NULL AND email IS NOT NULL ORDER BY username, email;
SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY email, username;
SELECT * FROM keys_test.explicit_unique_2_partial_index_test WHERE username IS NOT NULL ORDER BY email, username;
SELECT * FROM keys_test.explicit_unique_2_partial_index_test WHERE username IS NOT NULL AND email IS NOT NULL ORDER BY email, username;
SELECT * FROM keys_test.explicit_unique_2_partial_index_test ORDER BY email;
SELECT * FROM keys_test.explicit_unique_2_partial_index_test WHERE email IS NOT NULL ORDER BY email;

-- this query should use the index
SELECT * FROM keys_test.explicit_unique_1_func_index_test ORDER BY username;
SELECT * FROM keys_test.explicit_unique_2_func_index_test ORDER BY username;
-- this query should use the index
SELECT * FROM keys_test.explicit_unique_2_func_index_test ORDER BY username, email;
SELECT * FROM keys_test.explicit_unique_2_func_index_test ORDER BY email, username;
SELECT * FROM keys_test.explicit_unique_2_func_index_test ORDER BY email;

-- select using different indicies
-- use primary key
SELECT * FROM keys_test.employees ORDER BY employee_id;
-- use uniq_employees_email index
SELECT * FROM keys_test.employees ORDER BY email;
-- use uniq_employees_phone index
SELECT * FROM keys_test.employees ORDER BY phone_number;
-- use uniq_employees_national_id index
SELECT * FROM keys_test.employees ORDER BY national_id;
-- use uniq_employees_dept_location index
SELECT * FROM keys_test.employees ORDER BY department, location;
-- do not use any index
SELECT * FROM keys_test.employees ORDER BY location, department;


