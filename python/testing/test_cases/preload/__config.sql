## setup

-- create table with toast data
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS preload_random_binary_test (
    id SERIAL PRIMARY KEY,
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS preload_random_text_test (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL
);

-- insert into preload_random_binary_test
WITH chunk AS (SELECT gen_random_bytes(1024) AS b)
INSERT INTO preload_random_binary_test (data)
SELECT b || b || b || b || b || b || b || b || b || b FROM chunk
CROSS JOIN generate_series(1, 10);

-- insert into preload_random_text_test
INSERT INTO preload_random_text_test (data)
SELECT string_agg(chr(65 + trunc(random() * 94)::int), '')
FROM generate_series(1, 10) AS gs1(id)
JOIN generate_series(1, 100000) AS gs2(id2) ON TRUE
GROUP BY gs1.id;

-- data types schema
CREATE SCHEMA data_type_test;

-- table with number types
CREATE TABLE data_type_test.all_numeric_types (
    smallint_col        smallint,
    integer_col         integer,
    bigint_col          bigint,
    decimal_col         decimal(10, 2),
    numeric_col         numeric(12, 4),
    real_col            real,
    double_precision_col double precision,
    serial_col          serial,
    bigserial_col       bigserial
);

INSERT INTO data_type_test.all_numeric_types (
    smallint_col,
    integer_col,
    bigint_col,
    decimal_col,
    numeric_col,
    real_col,
    double_precision_col
)
SELECT
    1000 + i, 
    100000 + i,
    10000000000 + i,
    100.00 + i * 1.11,
    1000.1234 + i * 0.01,
    1.5 + i * 0.1,
    3.1415 + i * 0.0001
FROM generate_series(1, 11) AS s(i);

-- table with character and binary types and boolean
CREATE TABLE data_type_test.all_char_and_binary_types (
    id SERIAL PRIMARY KEY,

    char_col CHAR(10),
    varchar_col VARCHAR(50),
    text_col TEXT,

    bytea_col BYTEA,

    bool_col BOOLEAN
);

INSERT INTO data_type_test.all_char_and_binary_types (char_col, varchar_col, text_col, bytea_col, bool_col)
VALUES 
    ('char1    ', 'varchar1', 'text1', decode('0011223344', 'hex'), TRUE),
    ('char2    ', 'varchar2', 'text2', decode('AABBCCDD', 'hex'), FALSE),
    ('char3    ', 'varchar3', 'text3', decode('DEADBEEF', 'hex'), TRUE),
    ('char4    ', 'varchar4', 'text4', decode('CAFEBABE', 'hex'), FALSE),
    ('char5    ', 'varchar5', 'text5', decode('FEEDFACE', 'hex'), TRUE);

-- table with time and date data types

CREATE TABLE data_type_test.all_datetime_types (
    id SERIAL PRIMARY KEY,

    date_col DATE,
    time_col TIME,
    time_tz_col TIME WITH TIME ZONE,
    timestamp_col TIMESTAMP,
    timestamp_tz_col TIMESTAMP WITH TIME ZONE,
    interval_col INTERVAL
);
-- 
INSERT INTO data_type_test.all_datetime_types (
    date_col,
    time_col,
    time_tz_col,
    timestamp_col,
    timestamp_tz_col,
    interval_col
)
SELECT 
    CURRENT_DATE + i,
    CURRENT_TIME + (i || ' minutes')::interval,
    (CURRENT_TIME + (i || ' minutes')::interval) AT TIME ZONE 'UTC',
    CURRENT_TIMESTAMP + (i || ' minutes')::interval,
    (CURRENT_TIMESTAMP + (i || ' minutes')::interval) AT TIME ZONE 'UTC',
    (i || ' hours')::interval
FROM generate_series(0, 10) AS i;

-- table with user defined enum, uuid, and bit strings
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

-- geometric types

CREATE TABLE data_type_test.geometric_types_table (
    id SERIAL PRIMARY KEY,
    point_col POINT,
    line_col LINE,
    lseg_col LSEG,
    box_col BOX,
    path_col PATH,
    polygon_col POLYGON,
    circle_col CIRCLE
);

INSERT INTO data_type_test.geometric_types_table (point_col, line_col, lseg_col, box_col, path_col, polygon_col, circle_col) VALUES
    ('(1,2)', '{1,-1,0}', '[(1,1),(3,3)]', '((0,0),(4,4))', '[(0,0),(1,1),(2,1)]', '((0,0),(2,0),(2,2),(0,2))', '<(5,5),3>'),
    ('(3,4)', '{1,-1,-1}', '[(2,2),(4,4)]', '((1,1),(5,5))', '[(1,1),(2,2),(3,1)]', '((1,1),(3,1),(3,3),(1,3))', '<(10,10),5>'),
    ('(5,6)', '{2,-1,-3}', '[(3,3),(5,5)]', '((2,2),(6,6))', '[(2,2),(3,3),(4,2)]', '((2,2),(4,2),(4,4),(2,4))', '<(15,15),7>'),
    ('(7,8)', '{3,-1,-7}', '[(4,4),(6,6)]', '((3,3),(7,7))', '[(3,3),(4,4),(5,3)]', '((3,3),(5,3),(5,5),(3,5))', '<(20,20),2>'),
    ('(9,10)', '{4,-1,-11}', '[(5,5),(7,7)]', '((4,4),(8,8))', '[(4,4),(5,5),(6,4)]', '((4,4),(6,4),(6,6),(4,6))', '<(25,25),6>'),
    ('(11,12)', '{5,-1,-17}', '[(6,6),(8,8)]', '((5,5),(9,9))', '[(5,5),(6,6),(7,5)]', '((5,5),(7,5),(7,7),(5,7))', '<(30,30),4>'),
    ('(13,14)', '{6,-1,-23}', '[(7,7),(9,9)]', '((6,6),(10,10))', '[(6,6),(7,7),(8,6)]', '((6,6),(8,6),(8,8),(6,8))', '<(35,35),1>'),
    ('(15,16)', '{7,-1,-29}', '[(8,8),(10,10)]', '((7,7),(11,11))', '[(7,7),(8,8),(9,7)]', '((7,7),(9,7),(9,9),(7,9))', '<(40,40),3>'),
    ('(17,18)', '{8,-1,-35}', '[(9,9),(11,11)]', '((8,8),(12,12))', '[(8,8),(9,9),(10,8)]', '((8,8),(10,8),(10,10),(8,10))', '<(45,45),8>'),
    ('(19,20)', '{9,-1,-41}', '[(10,10),(12,12)]', '((9,9),(13,13))', '[(9,9),(10,10),(11,9)]', '((9,9),(11,9),(11,11),(9,11))', '<(50,50),9>'),
    ('(21,22)', '{10,-1,-47}', '[(11,11),(13,13)]', '((10,10),(14,14))', '[(10,10),(11,11),(12,10)]', '((10,10),(12,10),(12,12),(10,12))', '<(55,55),10>'),
    ('(23,24)', '{11,-1,-53}', '[(12,12),(14,14)]', '((11,11),(15,15))', '[(11,11),(12,12),(13,11)]', '((11,11),(13,11),(13,13),(11,13))', '<(60,60),11>'),
    ('(25,26)', '{12,-1,-59}', '[(13,13),(15,15)]', '((12,12),(16,16))', '[(12,12),(13,13),(14,12)]', '((12,12),(14,12),(14,14),(12,14))', '<(65,65),12>'),
    ('(27,28)', '{13,-1,-65}', '[(14,14),(16,16)]', '((13,13),(17,17))', '[(13,13),(14,14),(15,13)]', '((13,13),(15,13),(15,15),(13,15))', '<(70,70),13>'),
    ('(29,30)', '{14,-1,-71}', '[(15,15),(17,17)]', '((14,14),(18,18))', '[(14,14),(15,15),(16,14)]', '((14,14),(16,14),(16,16),(14,16))', '<(75,75),14>');

-- network address type and array

CREATE TABLE data_type_test.network_types_table (
    inet_col inet,
    cidr_col cidr,
    macaddr_col macaddr,
    macaddr8_col macaddr8,
    inet_array inet[]
);

INSERT INTO data_type_test.network_types_table (inet_col, cidr_col, macaddr_col, macaddr8_col, inet_array) VALUES
    ('192.168.1.10',     '192.168.1.0/24', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05', ARRAY['192.168.1.1', '10.0.0.1']::inet[]),
    ('10.0.0.55',        '10.0.0.0/8',     '00:1a:2b:3c:4d:5e', '00:1a:2b:3c:4d:5e:6f:70', ARRAY['10.0.0.10', '172.16.0.1']::inet[]),
    ('172.16.5.4',       '172.16.0.0/16',  'aa:bb:cc:dd:ee:ff', 'aa:bb:cc:dd:ee:ff:11:22', ARRAY['172.16.5.1', '192.168.0.1']::inet[]),
    ('2001:db8::1',      '2001:db8::/32',  'de:ad:be:ef:00:01', 'de:ad:be:ef:00:01:02:03', ARRAY['2001:db8::2', '2001:db8::3']::inet[]),
    ('fe80::1234',       'fe80::/10',      '12:34:56:78:9a:bc', '12:34:56:78:9a:bc:de:f0', ARRAY['fe80::1', 'fe80::2']::inet[]),
    ('127.0.0.1',        '127.0.0.0/8',    '00:00:5e:00:53:01', '00:00:5e:00:53:01:02:03', ARRAY['127.0.0.1', '192.0.2.1']::inet[]),
    ('192.0.2.100',      '192.0.2.0/24',   'ff:ff:ff:ff:ff:ff', 'ff:ff:ff:ff:ff:ff:ff:ff', ARRAY['192.0.2.1', '198.51.100.1']::inet[]),
    ('198.51.100.50',    '198.51.100.0/24','01:23:45:67:89:ab', '01:23:45:67:89:ab:cd:ef', ARRAY['198.51.100.1', '203.0.113.1']::inet[]),
    ('203.0.113.25',     '203.0.113.0/24', '10:20:30:40:50:60', '10:20:30:40:50:60:70:80', ARRAY['203.0.113.1', '224.0.0.1']::inet[]),
    ('224.0.0.1',        '224.0.0.0/4',    'aa:bb:cc:dd:ee:01', 'aa:bb:cc:dd:ee:01:02:03', ARRAY['224.0.0.2', '239.255.255.255']::inet[]),
    ('255.255.255.255',  '255.255.255.255/32','01:80:c2:00:00:00', '01:80:c2:00:00:00:00:00', ARRAY['255.255.255.254', '255.255.255.253']::inet[]),
    ('0.0.0.0',          '0.0.0.0/0',      '00:11:22:33:44:55', '00:11:22:33:44:55:66:77', ARRAY['0.0.0.1', '0.0.0.2']::inet[]);

-- table with table with text search types, json, and jsonb types

CREATE TABLE data_type_test.text_search_json (
    id SERIAL PRIMARY KEY,
    tsv_col tsvector,
    tsq_col tsquery,
    json_col json,
    jsonb_col jsonb
);

INSERT INTO data_type_test.text_search_json (tsv_col, tsq_col, json_col, jsonb_col) VALUES
    (
        to_tsvector('english', 'Postgres supports json and jsonb data types'),
        to_tsquery('english', 'Postgres & data'),
        '{"name": "John", "skills": ["sql", "plpgsql"]}',
        '{"name": "John", "skills": ["sql", "plpgsql"], "level": "advanced"}'
    ),
    (
        to_tsvector('english', 'JSONB allows indexing and efficient queries'),
        to_tsquery('english', 'indexing | efficient'),
        '{"active": true, "score": 42}',
        '{"active": true, "score": 42, "tags": ["postgres", "jsonb"]}'
    ),
    (
        to_tsvector('english', 'TSVector and TSQuery with JSON'),
        to_tsquery('english', 'TSVector & TSQuery'),
        '{"config": {"enabled": false, "timeout": 100}}',
        '{"config": {"enabled": true, "timeout": 150}}'
    ),
    (
        to_tsvector('english', 'Mixing data types in JSON columns'),
        to_tsquery('english', 'Mixing & data'),
        '{"items": [1, 2, 3], "flag": "yes"}',
        '{"items": [1, 2, 3], "flag": true}'
    ),
    (
        to_tsvector('english', 'Postgres JSON vs JSONB differences'),
        to_tsquery('english', 'Postgres & differences'),
        '{"nullValue": null, "text": "example"}',
        '{"nullValue": null, "text": "example", "extra": {"nested": 1}}'
    );

-- table with composite type, range types, and monetary type

CREATE TYPE data_type_test.person_info AS (
    first_name text,
    last_name text,
    age int
);

CREATE TABLE data_type_test.composite_data (
    id serial PRIMARY KEY,
    info data_type_test.person_info,
    price money,
    int_range int4range,
    ts_range tsrange,
    num_range numrange
);

INSERT INTO data_type_test.composite_data (info, price, int_range, ts_range, num_range) VALUES
    (ROW('Eve', 'Adams', 29), '$75.00', '[1,5)', '[2025-05-01 09:00, 2025-05-01 11:00)', '[0.1,1.0)'),
    (ROW('Frank', 'Baker', 52), '$250.25', '[100,200)', '[2025-06-15 13:00, 2025-06-15 15:30)', '[3.3,7.7)'),
    (ROW('Grace', 'Clark', 40), '$600.00', '[0,50)', '[2025-07-20 08:30, 2025-07-20 12:00)', '[10.5,20.5)'),
    (ROW('Hank', 'Davis', 33), '$1234.56', '[25,35]', '[2025-08-10 14:00, 2025-08-10 16:00)', '[7,14)'),
    (ROW('Ivy', 'Evans', 22), '$89.99', '[15,45)', '[2025-09-05 07:00, 2025-09-05 09:00)', '[2,5)'),
    (ROW('Jack', 'Foster', 60), '$1500.00', '[40,60)', '[2025-10-10 10:00, 2025-10-10 12:00)', '[0,3.14)'),
    (ROW('Karen', 'Green', 38), '$432.10', '[10,20)', '[2025-11-11 18:00, 2025-11-11 20:00)', '[1,2)'),
    (ROW('Leo', 'Hall', 44), '$777.77', '[5,15)', '[2025-12-12 08:00, 2025-12-12 10:30)', '[4,8)'),
    (ROW('Mia', 'Irwin', 27), '$99.99', '[20,30]', '[2026-01-01 00:00, 2026-01-01 02:00)', '[0,1)'),
    (ROW('Ned', 'Jones', 50), '$111.11', '[30,40)', '[2026-02-02 14:00, 2026-02-02 16:00)', '[5,9)');

CREATE TABLE data_type_test.special_data (
    id serial PRIMARY KEY,
    -- info data_type_test.person_info,
    price money,
    int_range int4range,
    ts_range tsrange,
    num_range numrange
);

INSERT INTO data_type_test.special_data (price, int_range, ts_range, num_range) VALUES
    ('$75.00', '[1,5)', '[2025-05-01 09:00, 2025-05-01 11:00)', '[0.1,1.0)'),
    ('$250.25', '[100,200)', '[2025-06-15 13:00, 2025-06-15 15:30)', '[3.3,7.7)'),
    ('$600.00', '[0,50)', '[2025-07-20 08:30, 2025-07-20 12:00)', '[10.5,20.5)'),
    ('$1234.56', '[25,35]', '[2025-08-10 14:00, 2025-08-10 16:00)', '[7,14)'),
    ('$89.99', '[15,45)', '[2025-09-05 07:00, 2025-09-05 09:00)', '[2,5)'),
    ('$1500.00', '[40,60)', '[2025-10-10 10:00, 2025-10-10 12:00)', '[0,3.14)'),
    ('$432.10', '[10,20)', '[2025-11-11 18:00, 2025-11-11 20:00)', '[1,2)'),
    ('$777.77', '[5,15)', '[2025-12-12 08:00, 2025-12-12 10:30)', '[4,8)'),
    ('$99.99', '[20,30]', '[2026-01-01 00:00, 2026-01-01 02:00)', '[0,1)'),
    ('$111.11', '[30,40)', '[2026-02-02 14:00, 2026-02-02 16:00)', '[5,9)');

-- table with all range and multyrange types

CREATE TABLE data_type_test.range_multirange_test (
    id serial PRIMARY KEY,
    int4_range int4range,
    int8_range int8range,
    num_range numrange,
    ts_range tsrange,
    tstz_range tstzrange,
    date_range daterange,
    
    int4_multirange int4multirange,
    int8_multirange int8multirange,
    num_multirange nummultirange,
    ts_multirange tsmultirange,
    tstz_multirange tstzmultirange,
    date_multirange datemultirange
);

INSERT INTO data_type_test.range_multirange_test (
    int4_range, int8_range, num_range, ts_range, tstz_range, date_range,
    int4_multirange, int8_multirange, num_multirange, ts_multirange, tstz_multirange, date_multirange
) VALUES
(
    '[1,10)', '[100,200)', '[1.1,2.2)', 
    '[2023-01-01 10:00, 2023-01-01 11:00)', '[2023-01-01 10:00+00, 2023-01-01 12:00+00)', '[2023-01-01, 2023-01-15)',
    '{[1,5),[7,10)}', '{[100,150),[160,180)}', '{[1.1,1.5),[1.7,2.0)}',
    '{[2023-01-01 10:00, 2023-01-01 10:30), [2023-01-01 10:45, 2023-01-01 11:00)}',
    '{[2023-01-01 10:00+00, 2023-01-01 11:00+00), [2023-01-01 11:30+00, 2023-01-01 12:00+00)}',
    '{[2023-01-01, 2023-01-05), [2023-01-10, 2023-01-15)}'
),
(
    '[20,30)', '[300,400)', '[3.14,6.28)',
    '[2024-02-01 08:00, 2024-02-01 09:00)', '[2024-02-01 08:00+00, 2024-02-01 09:00+00)', '[2024-02-01, 2024-02-10)',
    '{[20,22),[25,28)}', '{[300,350),[360,380)}', '{[3.14,4.0),[5.0,6.0)}',
    '{[2024-02-01 08:00, 2024-02-01 08:30), [2024-02-01 08:45, 2024-02-01 09:00)}',
    '{[2024-02-01 08:00+00, 2024-02-01 08:45+00), [2024-02-01 08:50+00, 2024-02-01 09:00+00)}',
    '{[2024-02-01, 2024-02-05), [2024-02-07, 2024-02-10)}'
),
(
    '[100,110)', '[1000,1200)', '[10.1,12.2)',
    '[2025-03-15 12:00, 2025-03-15 13:00)', '[2025-03-15 12:00+00, 2025-03-15 13:00+00)', '[2025-03-15, 2025-03-20)',
    '{[100,105),[107,109)}', '{[1000,1100),[1150,1170)}', '{[10.1,11.0),[11.5,12.0)}',
    '{[2025-03-15 12:00, 2025-03-15 12:30), [2025-03-15 12:35, 2025-03-15 13:00)}',
    '{[2025-03-15 12:00+00, 2025-03-15 12:30+00), [2025-03-15 12:45+00, 2025-03-15 13:00+00)}',
    '{[2025-03-15, 2025-03-17), [2025-03-18, 2025-03-20)}'
),
(
    '[5,15)', '[500,600)', '[2.2,3.3)',
    '[2026-04-10 07:00, 2026-04-10 08:00)', '[2026-04-10 07:00+00, 2026-04-10 08:00+00)', '[2026-04-10, 2026-04-15)',
    '{[5,7),[10,12)}', '{[500,550),[560,580)}', '{[2.2,2.5),[2.7,3.0)}',
    '{[2026-04-10 07:00, 2026-04-10 07:30), [2026-04-10 07:45, 2026-04-10 08:00)}',
    '{[2026-04-10 07:00+00, 2026-04-10 07:30+00), [2026-04-10 07:50+00, 2026-04-10 08:00+00)}',
    '{[2026-04-10, 2026-04-12), [2026-04-13, 2026-04-15)}'
),
(
    '[50,60)', '[700,800)', '[7.7,8.8)',
    '[2027-05-20 15:00, 2027-05-20 16:00)', '[2027-05-20 15:00+00, 2027-05-20 16:00+00)', '[2027-05-20, 2027-05-25)',
    '{[50,55),[57,59)}', '{[700,750),[760,780)}', '{[7.7,8.0),[8.1,8.7)}',
    '{[2027-05-20 15:00, 2027-05-20 15:30), [2027-05-20 15:40, 2027-05-20 16:00)}',
    '{[2027-05-20 15:00+00, 2027-05-20 15:45+00), [2027-05-20 15:50+00, 2027-05-20 16:00+00)}',
    '{[2027-05-20, 2027-05-22), [2027-05-23, 2027-05-25)}'
),
(
    '[10,20)', '[150,250)', '[4.5,5.5)',
    '[2028-06-30 09:00, 2028-06-30 10:00)', '[2028-06-30 09:00+00, 2028-06-30 10:00+00)', '[2028-06-30, 2028-07-05)',
    '{[10,15),[17,19)}', '{[150,200),[210,240)}', '{[4.5,5.0),[5.2,5.4)}',
    '{[2028-06-30 09:00, 2028-06-30 09:30), [2028-06-30 09:45, 2028-06-30 10:00)}',
    '{[2028-06-30 09:00+00, 2028-06-30 09:45+00), [2028-06-30 09:50+00, 2028-06-30 10:00+00)}',
    '{[2028-06-30, 2028-07-02), [2028-07-03, 2028-07-05)}'
),
(
    '[200,210)', '[1300,1400)', '[20.0,22.0)',
    '[2029-07-15 14:00, 2029-07-15 15:00)', '[2029-07-15 14:00+00, 2029-07-15 15:00+00)', '[2029-07-15, 2029-07-20)',
    '{[200,205),[207,209)}', '{[1300,1350),[1360,1380)}', '{[20.0,21.0),[21.5,21.9)}',
    '{[2029-07-15 14:00, 2029-07-15 14:30), [2029-07-15 14:35, 2029-07-15 15:00)}',
    '{[2029-07-15 14:00+00, 2029-07-15 14:30+00), [2029-07-15 14:45+00, 2029-07-15 15:00+00)}',
    '{[2029-07-15, 2029-07-17), [2029-07-18, 2029-07-20)}'
),
(
    '[7,14)', '[600,700)', '[3.3,4.4)',
    '[2030-08-01 06:00, 2030-08-01 07:00)', '[2030-08-01 06:00+00, 2030-08-01 07:00+00)', '[2030-08-01, 2030-08-06)',
    '{[7,9),[11,13)}', '{[600,650),[660,680)}', '{[3.3,3.9),[4.1,4.3)}',
    '{[2030-08-01 06:00, 2030-08-01 06:30), [2030-08-01 06:40, 2030-08-01 07:00)}',
    '{[2030-08-01 06:00+00, 2030-08-01 06:30+00), [2030-08-01 06:35+00, 2030-08-01 07:00+00)}',
    '{[2030-08-01, 2030-08-03), [2030-08-04, 2030-08-06)}'
),
(
    '[25,35)', '[900,1000)', '[5.5,6.5)',
    '[2031-09-10 13:00, 2031-09-10 14:00)', '[2031-09-10 13:00+00, 2031-09-10 14:00+00)', '[2031-09-10, 2031-09-15)',
    '{[25,30),[32,34)}', '{[900,950),[960,980)}', '{[5.5,6.0),[6.2,6.4)}',
    '{[2031-09-10 13:00, 2031-09-10 13:30), [2031-09-10 13:40, 2031-09-10 14:00)}',
    '{[2031-09-10 13:00+00, 2031-09-10 13:45+00), [2031-09-10 13:50+00, 2031-09-10 14:00+00)}',
    '{[2031-09-10, 2031-09-12), [2031-09-13, 2031-09-15)}'
),
(
    '[15,25)', '[400,500)', '[6.6,7.7)',
    '[2032-10-05 11:00, 2032-10-05 12:00)', '[2032-10-05 11:00+00, 2032-10-05 12:00+00)', '[2032-10-05, 2032-10-10)',
    '{[15,18),[20,23)}', '{[400,450),[460,480)}', '{[6.6,7.0),[7.2,7.6)}',
    '{[2032-10-05 11:00, 2032-10-05 11:30), [2032-10-05 11:45, 2032-10-05 12:00)}',
    '{[2032-10-05 11:00+00, 2032-10-05 11:45+00), [2032-10-05 11:50+00, 2032-10-05 12:00+00)}',
    '{[2032-10-05, 2032-10-07), [2032-10-08, 2032-10-10)}'
),
(
    '[12,22)', '[250,350)', '[8.8,9.9)',
    '[2033-11-20 10:00, 2033-11-20 11:00)', '[2033-11-20 10:00+00, 2033-11-20 11:00+00)', '[2033-11-20, 2033-11-25)',
    '{[12,16),[18,21)}', '{[250,300),[310,330)}', '{[8.8,9.0),[9.3,9.8)}',
    '{[2033-11-20 10:00, 2033-11-20 10:30), [2033-11-20 10:40, 2033-11-20 11:00)}',
    '{[2033-11-20 10:00+00, 2033-11-20 10:30+00), [2033-11-20 10:35+00, 2033-11-20 11:00+00)}',
    '{[2033-11-20, 2033-11-22), [2033-11-23, 2033-11-25)}'
);

-- keys schema
CREATE SCHEMA keys_test;

-- one column PK and UK
CREATE TABLE keys_test.table1 (
    id INT,
    col1 TEXT,
    col2 TEXT,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX uniq_table1_col1 ON keys_test.table1 (col1);

-- table1: primary key (id), unique index on (col1)
INSERT INTO keys_test.table1 (id, col1, col2) VALUES
    (1, 'uniqueA', 'foo1'),
    (2, 'uniqueB', 'foo2'),
    (3, 'uniqueC', 'foo3'),
    (4, 'uniqueD', 'foo4'),
    (5, 'uniqueE', 'foo5'),
    (6, 'uniqueF', 'foo6'),
    (7, 'uniqueG', 'foo7'),
    (8, 'uniqueH', 'foo8'),
    (9, 'uniqueI', 'foo9'),
    (10, 'uniqueJ', 'foo10');

-- two columnis PK and UK
CREATE TABLE keys_test.table2 (
    id1 INT,
    id2 INT,
    col1 TEXT,
    col2 TEXT,
    col3 TEXT,
    PRIMARY KEY (id1, id2)
);

CREATE UNIQUE INDEX uniq_table2_col1_col2 ON keys_test.table2 (col1, col2);

-- table2: primary key (id1, id2), unique index on (col1, col2)
INSERT INTO keys_test.table2 (id1, id2, col1, col2, col3) VALUES
    (1, 1, 'alpha', 'beta', 'val1'),
    (1, 2, 'gamma', 'delta', 'val2'),
    (2, 1, 'epsilon', 'zeta', 'val3'),
    (2, 2, 'eta', 'theta', 'val4'),
    (3, 1, 'iota', 'kappa', 'val5'),
    (3, 2, 'lambda', 'mu', 'val6'),
    (4, 1, 'nu', 'xi', 'val7'),
    (4, 2, 'omicron', 'pi', 'val8'),
    (5, 1, 'rho', 'sigma', 'val9'),
    (5, 2, 'tau', 'upsilon', 'val10');

-- three columnis PK and UK
CREATE TABLE keys_test.table3 (
    id1 INT,
    id2 INT,
    id3 INT,
    col1 TEXT,
    col2 TEXT,
    col3 TEXT,
    col4 TEXT,
    PRIMARY KEY (id1, id2, id3)
);

CREATE UNIQUE INDEX uniq_table3_col1_col2_col3 ON keys_test.table3 (col1, col2, col3);

-- table3: primary key (id1, id2, id3), unique index on (col1, col2, col3)
INSERT INTO keys_test.table3 (id1, id2, id3, col1, col2, col3, col4) VALUES
    (1, 1, 1, 'red', 'green', 'blue', 'extra1'),
    (1, 1, 2, 'cyan', 'magenta', 'yellow', 'extra2'),
    (1, 2, 1, 'black', 'white', 'gray', 'extra3'),
    (1, 2, 2, 'orange', 'purple', 'brown', 'extra4'),
    (2, 1, 1, 'pink', 'lime', 'navy', 'extra5'),
    (2, 1, 2, 'teal', 'maroon', 'olive', 'extra6'),
    (2, 2, 1, 'silver', 'gold', 'bronze', 'extra7'),
    (2, 2, 2, 'coral', 'turquoise', 'lavender', 'extra8'),
    (3, 1, 1, 'peach', 'plum', 'cherry', 'extra9'),
    (3, 1, 2, 'mint', 'cream', 'mustard', 'extra10');

-- unique index: 1 column, not null
CREATE TABLE keys_test.explicit_unique_1_index_test (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE UNIQUE INDEX explicit_unique_1_index_test_idx
    ON keys_test.explicit_unique_1_index_test (username);

INSERT INTO keys_test.explicit_unique_1_index_test (username, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.net'),
    ('Diana', 'diana@sample.org'),
    ('Eve', 'eve@example.com'),
    ('Frank', 'frank@domain.com'),
    ('Grace', 'grace@demo.net'),
    ('Heidi', 'heidi@sample.com'),
    ('Ivan', 'ivan@another.com'),
    ('Judy', 'judy@example.org'),
    ('Mallory', 'mallory@example.com'),
    ('Niaj', 'niaj@domain.org'),
    ('Oscar', 'oscar@sample.net'),
    ('Peggy', 'peggy@demo.org'),
    ('Trent', 'trent@service.com');

-- unique index: 2 columns, not null
CREATE TABLE keys_test.explicit_unique_2_index_test (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE UNIQUE INDEX explicit_unique_2_index_test_idx
    ON keys_test.explicit_unique_2_index_test (username, email);

INSERT INTO keys_test.explicit_unique_2_index_test (username, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.net'),
    ('Diana', 'diana@sample.org'),
    ('Eve', 'eve@example.com'),
    ('Frank', 'frank@domain.com'),
    ('Grace', 'grace@demo.net'),
    ('Heidi', 'heidi@sample.com'),
    ('Ivan', 'ivan@another.com'),
    ('Judy', 'judy@example.org'),
    ('Mallory', 'mallory@example.com'),
    ('Niaj', 'niaj@domain.org'),
    ('Oscar', 'oscar@sample.net'),
    ('Peggy', 'peggy@demo.org'),
    ('Trent', 'trent@service.com');

-- unique index: 1 column, with null
CREATE TABLE keys_test.explicit_unique_1_null_index_test (
    id SERIAL PRIMARY KEY,
    username TEXT,
    email TEXT
);

CREATE UNIQUE INDEX explicit_unique_1_null_index_test_idx
    ON keys_test.explicit_unique_1_null_index_test (username);

INSERT INTO keys_test.explicit_unique_1_null_index_test (username, email) VALUES
    ('Alice',    'alice@example.com'),
    ('Bob',      'bob@example.com'),
    (NULL,       'ghost1@example.com'),
    ('Charlie',  NULL),
    (NULL,       NULL),
    ('Diana',    'diana@example.org'),
    ('Eve',      'eve@example.net'),
    ('Frank',    'frank@example.net'),
    ('Grace',    NULL),
    ('Heidi',    'heidi@example.org'),
    (NULL,       'ghost2@example.com'),
    ('Ivan',     'ivan@example.org'),
    ('Judy',     NULL),
    ('Mallory',  'mallory@foo.com'),
    ('Niaj',     'niaj@bar.com');

-- unique index: 2 columns, with null
CREATE TABLE keys_test.explicit_unique_2_null_index_test (
    id SERIAL PRIMARY KEY,
    username TEXT,
    email TEXT
);

CREATE UNIQUE INDEX explicit_unique_2_null_index_test_idx
    ON keys_test.explicit_unique_2_null_index_test (username, email);

INSERT INTO keys_test.explicit_unique_2_null_index_test (username, email) VALUES
    ('Alice',    'alice@example.com'),
    ('Bob',      'bob@example.com'),
    (NULL,       'ghost1@example.com'),
    ('Charlie',  NULL),
    (NULL,       NULL),
    ('Diana',    'diana@example.org'),
    ('Eve',      'eve@example.net'),
    ('Frank',    'frank@example.net'),
    ('Grace',    NULL),
    ('Heidi',    'heidi@example.org'),
    (NULL,       'ghost2@example.com'),
    ('Ivan',     'ivan@example.org'),
    ('Judy',     NULL),
    ('Mallory',  'mallory@foo.com'),
    ('Niaj',     'niaj@bar.com');

-- unique index: 1 column, partial
CREATE TABLE keys_test.explicit_unique_1_partial_index_test (
    id SERIAL PRIMARY KEY,
    username TEXT,
    email TEXT
);

CREATE UNIQUE INDEX explicit_unique_1_partial_index_test_idx
    ON keys_test.explicit_unique_1_partial_index_test (username)
    WHERE username IS NOT NULL;

INSERT INTO keys_test.explicit_unique_1_partial_index_test (username, email) VALUES
    ('Alice',    'alice@example.com'),
    ('Bob',      'bob@example.com'),
    (NULL,       'ghost1@example.com'),
    ('Charlie',  NULL),
    (NULL,       NULL),
    ('Diana',    'diana@example.org'),
    ('Eve',      'eve@example.net'),
    ('Frank',    'frank@example.net'),
    ('Grace',    NULL),
    ('Heidi',    'heidi@example.org'),
    (NULL,       'ghost2@example.com'),
    ('Ivan',     'ivan@example.org'),
    ('Judy',     NULL),
    ('Mallory',  'mallory@foo.com'),
    ('Niaj',     'niaj@bar.com');

-- unique index: 2 columns, partial
CREATE TABLE keys_test.explicit_unique_2_partial_index_test (
    id SERIAL PRIMARY KEY,
    username TEXT,
    email TEXT
);

CREATE UNIQUE INDEX explicit_unique_2_partial_index_test_idx
    ON keys_test.explicit_unique_2_partial_index_test (username, email)
    WHERE username IS NOT NULL AND email IS NOT NULL;

INSERT INTO keys_test.explicit_unique_2_partial_index_test (username, email) VALUES
    ('Alice',    'alice@example.com'),
    ('Bob',      'bob@example.com'),
    (NULL,       'ghost1@example.com'),
    ('Charlie',  NULL),
    (NULL,       NULL),
    ('Diana',    'diana@example.org'),
    ('Eve',      'eve@example.net'),
    ('Frank',    'frank@example.net'),
    ('Grace',    NULL),
    ('Heidi',    'heidi@example.org'),
    (NULL,       'ghost2@example.com'),
    ('Ivan',     'ivan@example.org'),
    ('Judy',     NULL),
    ('Mallory',  'mallory@foo.com'),
    ('Niaj',     'niaj@bar.com');

-- unique index: 1 column, functional
CREATE TABLE keys_test.explicit_unique_1_func_index_test (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE UNIQUE INDEX explicit_unique_1_func_index_test_idx
    ON keys_test.explicit_unique_1_func_index_test (UPPER(username));

INSERT INTO keys_test.explicit_unique_1_func_index_test (username, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.net'),
    ('Diana', 'diana@sample.org'),
    ('Eve', 'eve@example.com'),
    ('Frank', 'frank@domain.com'),
    ('Grace', 'grace@demo.net'),
    ('Heidi', 'heidi@sample.com'),
    ('Ivan', 'ivan@another.com'),
    ('Judy', 'judy@example.org'),
    ('Mallory', 'mallory@example.com'),
    ('Niaj', 'niaj@domain.org'),
    ('Oscar', 'oscar@sample.net'),
    ('Peggy', 'peggy@demo.org'),
    ('Trent', 'trent@service.com');

-- unique index: 2 columns, functional
CREATE TABLE keys_test.explicit_unique_2_func_index_test (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE UNIQUE INDEX explicit_unique_2_func_index_test_idx
    ON keys_test.explicit_unique_2_func_index_test (UPPER(username), UPPER(email));

INSERT INTO keys_test.explicit_unique_2_func_index_test (username, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.net'),
    ('Diana', 'diana@sample.org'),
    ('Eve', 'eve@example.com'),
    ('Frank', 'frank@domain.com'),
    ('Grace', 'grace@demo.net'),
    ('Heidi', 'heidi@sample.com'),
    ('Ivan', 'ivan@another.com'),
    ('Judy', 'judy@example.org'),
    ('Mallory', 'mallory@example.com'),
    ('Niaj', 'niaj@domain.org'),
    ('Oscar', 'oscar@sample.net'),
    ('Peggy', 'peggy@demo.org'),
    ('Trent', 'trent@service.com');


-- multiple unique indices
CREATE TABLE keys_test.employees (
    employee_id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    national_id TEXT NOT NULL,
    department TEXT NOT NULL,
    location TEXT NOT NULL,
    hire_date DATE NOT NULL
);

-- Unique index on email
CREATE UNIQUE INDEX uniq_employees_email ON keys_test.employees(email);

-- Unique index on phone_number
CREATE UNIQUE INDEX uniq_employees_phone ON keys_test.employees(phone_number);

-- Unique index on national_id
CREATE UNIQUE INDEX uniq_employees_national_id ON keys_test.employees(national_id);

-- Composite unique index on (department, location)
CREATE UNIQUE INDEX uniq_employees_dept_location ON keys_test.employees(department, location);

INSERT INTO keys_test.employees (email, phone_number, national_id, department, location, hire_date) VALUES
    ('alice.smith@example.com',     '555-0101', 'ID1001', 'Engineering',  'New York',     '2020-03-15'),
    ('bob.jones@example.org',       '555-0102', 'ID1002', 'HR',           'Chicago',      '2019-06-01'),
    ('carol.wilson@example.net',    '555-0103', 'ID1003', 'Finance',      'San Francisco','2021-07-22'),
    ('dave.brown@example.com',      '555-0104', 'ID1004', 'Engineering',  'Austin',       '2018-11-10'),
    ('eve.davis@example.org',       '555-0105', 'ID1005', 'Sales',        'Boston',       '2022-01-03'),
    ('frank.miller@example.net',    '555-0106', 'ID1006', 'Support',      'Seattle',      '2020-09-17'),
    ('grace.lee@example.com',       '555-0107', 'ID1007', 'Marketing',    'Denver',       '2017-05-25'),
    ('henry.adams@example.org',     '555-0108', 'ID1008', 'HR',           'Los Angeles',  '2016-12-30'),
    ('irene.clark@example.net',     '555-0109', 'ID1009', 'Finance',      'Miami',        '2023-04-05'),
    ('jack.taylor@example.com',     '555-0110', 'ID1010', 'Engineering',  'Chicago',      '2021-10-14');


## cleanup

-- cleanup toast tables
DROP TABLE IF EXISTS preload_random_binary_test CASCADE;
DROP TABLE IF EXISTS preload_random_text_test CASCADE;

DROP SCHEMA IF EXISTS data_type_test CASCADE;
DROP SCHEMA IF EXISTS keys_test CASCADE;


