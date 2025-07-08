## test
CREATE SCHEMA main_partition_schema;

CREATE SCHEMA child_partition_schema;

CREATE TABLE main_partition_schema.parent_table_001 (
    id SERIAL,
    order_id INT,
    order_date DATE,
    order_amount NUMERIC,
    PRIMARY KEY (id, order_date)
) PARTITION BY RANGE (order_date);

CREATE TABLE main_partition_schema.parent_table_002 (
    id SERIAL,
    order_id INT,
    order_date DATE,
    order_amount NUMERIC,
    PRIMARY KEY (id, order_date)
) PARTITION BY RANGE (order_date);

CREATE TABLE child_partition_schema.child_table_001_2023 PARTITION OF main_partition_schema.parent_table_001
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE child_partition_schema.child_table_001_2024 PARTITION OF main_partition_schema.parent_table_001
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE child_partition_schema.child_table_002_2024 PARTITION OF main_partition_schema.parent_table_002
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE child_partition_schema.child_table_002_2025 PARTITION OF main_partition_schema.parent_table_002
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- Detach the table from one partition table
ALTER TABLE main_partition_schema.parent_table_002
    DETACH PARTITION child_partition_schema.child_table_002_2025;

-- Attach the table to another partition table
ALTER TABLE main_partition_schema.parent_table_001
    ATTACH PARTITION child_partition_schema.child_table_002_2025 FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

INSERT INTO child_partition_schema.child_table_001_2023 (order_id, order_date, order_amount) VALUES
    (1001, '2023-01-01', 100.00);

INSERT INTO child_partition_schema.child_table_002_2024 (order_id, order_date, order_amount) VALUES
    (2002, '2024-01-01', 200.00);

## verify
### schema_check main_partition_schema parent_table_001
### schema_check main_partition_schema parent_table_002
### schema_check child_partition_schema child_table_001_2023
### schema_check child_partition_schema child_table_001_2024
### schema_check child_partition_schema child_table_002_2024
### schema_check child_partition_schema child_table_002_2025
SELECT * FROM main_partition_schema.parent_table_001 ORDER BY order_id, order_date;
SELECT * FROM main_partition_schema.parent_table_002 ORDER BY order_id, order_date;

## cleanup
DROP SCHEMA main_partition_schema CASCADE;
