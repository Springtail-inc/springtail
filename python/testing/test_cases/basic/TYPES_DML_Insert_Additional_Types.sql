## test
CREATE TABLE IF NOT EXISTS table_with_additional_types_insert (
    serial_id SERIAL PRIMARY KEY,
    binary_data BYTEA,
    json_column JSON,
    jsonb_column JSONB,
    uuid_column UUID DEFAULT gen_random_uuid(),
    ip_address INET,
    network_address CIDR,
    mac_address MACADDR,
    xml_data XML,
    monetary_value MONEY,
    integer_range INT4RANGE,
    numeric_range NUMRANGE,
    timestamp_range TSRANGE
);

-- Insert multiple rows
INSERT INTO table_with_additional_types_insert (
    binary_data,
    json_column,
    jsonb_column,
    uuid_column,
    ip_address,
    network_address,
    mac_address,
    xml_data,
    monetary_value,
    integer_range,
    numeric_range,
    timestamp_range
) VALUES
(
    decode('48656c6c6f', 'hex'), '{"key": "value"}', '{"key": true}', DEFAULT,
    '10.0.0.1', '10.0.0.0/16', 'AA:BB:CC:DD:EE:FF', '<tag>Example1</tag>', '$50.00', '[0,5]', '[5.5, 10.5]', '[2025-01-01, 2025-06-30]'
),
(
    decode('576f726c64', 'hex'), '{"another_key": 42}', '{"another_key": false}', DEFAULT,
    '172.16.0.1', '172.16.0.0/12', '11:22:33:44:55:66', '<tag>Example2</tag>', '$100.00', '[10,20]', '[15.5, 25.5]', '[2025-07-01, 2025-12-31]'
);

## verify
SELECT * FROM table_with_additional_types_insert;

## cleanup
DROP TABLE IF EXISTS table_with_additional_types_insert;
