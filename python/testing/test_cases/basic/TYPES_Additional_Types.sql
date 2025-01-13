## test
CREATE TABLE IF NOT EXISTS table_with_additional_types (
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

## verify
### schema_check public table_with_additional_types

## cleanup
DROP TABLE IF EXISTS table_with_additional_types;
