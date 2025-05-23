## setup
CREATE SCHEMA data_type_test;
CREATE TYPE data_type_test.mood AS ENUM (
    'happy',
    'sad',
    'neutral',
    'angry',
    'excited',
    'tired',
    'anxious',
    'confused',
    'bored',
    'calm',
    'frustrated',
    'hopeful',
    'nervous',
    'relaxed',
    'sleepy',
    'surprised',
    'content'
);

CREATE TABLE data_type_test.basic_types_table (
    id SERIAL PRIMARY KEY,
    varchar_col VARCHAR(255),
    char_col CHAR(10),
    name_col NAME,
    numeric_col NUMERIC(10, 2)
);

INSERT INTO data_type_test.basic_types_table (varchar_col, char_col, name_col, numeric_col)
VALUES
    ('Hello, World!', 'Hello', 'World', 1.23),
    ('PostgreSQL', 'Postgre', 'SQL', 4.56),
    ('Springtail', 'Spring', 'tail', 7.89);


CREATE TABLE data_type_test.array_test_table (
    id SERIAL PRIMARY KEY,
    int_array_col INT[],
    text_array_col TEXT[],
    bool_array_col BOOLEAN[]
);

INSERT INTO data_type_test.array_test_table (int_array_col, text_array_col, bool_array_col)
VALUES
    (ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'], ARRAY[TRUE, FALSE, TRUE]),
    (ARRAY[4, 5, 6], ARRAY['d', 'e', 'f'], ARRAY[FALSE, TRUE, FALSE]),
    (ARRAY[7, 8, 9], ARRAY['g', 'h', 'i'], ARRAY[TRUE, TRUE, FALSE]);

CREATE TABLE data_type_test.special_types_table (
    id SERIAL PRIMARY KEY,
    mood_col data_type_test.mood,
    uuid_col UUID,
    bit_col BIT(8),
    varbit_col BIT VARYING
);

INSERT INTO data_type_test.special_types_table (mood_col, uuid_col, bit_col, varbit_col)
VALUES
    ('happy', gen_random_uuid(), B'10101010', B'1101'),
    ('sad', gen_random_uuid(), B'11110000', B'101010101010'),
    ('neutral', gen_random_uuid(), B'00001111', B'1110001'),
    ('angry', gen_random_uuid(), B'01010101', B'10101'),
    ('excited', gen_random_uuid(), B'11111111', B'10011001'),
    ('bored', gen_random_uuid(), B'00110011', B'101001'),
    ('anxious', gen_random_uuid(), B'11001100', B'11110000'),
    ('content', gen_random_uuid(), B'10011001', B'1100110'),
    ('confused', gen_random_uuid(), B'01100110', B'1001'),
    ('relaxed', gen_random_uuid(), B'11100011', B'1111111'),
    ('frustrated', gen_random_uuid(), B'00011100', B'01010101'),
    ('surprised', gen_random_uuid(), B'10110110', B'1010'),
    ('nervous', gen_random_uuid(), B'11000011', B'1110'),
    ('confused', gen_random_uuid(), B'10010110', B'1000001'),
    ('sleepy', gen_random_uuid(), B'01010101', B'0001110');

## cleanup

DROP SCHEMA IF EXISTS data_type_test CASCADE;
