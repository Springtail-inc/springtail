## test
CREATE SCHEMA partition_table_schema_drop;

CREATE SCHEMA child_table_schema_drop;

CREATE TABLE partition_table_schema_drop.orders (
    id SERIAL,
    name TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    purchase_date DATE NOT NULL,
    PRIMARY KEY (id, purchase_date)
) PARTITION BY RANGE (purchase_date);

CREATE TABLE child_table_schema_drop.orders_2023 (
    id SERIAL,
    name TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    purchase_date DATE NOT NULL,
    PRIMARY KEY (id, purchase_date)
);

CREATE TABLE child_table_schema_drop.orders_2024 (
    id SERIAL,
    name TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    purchase_date DATE NOT NULL,
    PRIMARY KEY (id, purchase_date)
);

ALTER TABLE partition_table_schema_drop.orders
    ATTACH PARTITION child_table_schema_drop.orders_2023
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

ALTER TABLE partition_table_schema_drop.orders
    ATTACH PARTITION child_table_schema_drop.orders_2024
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

INSERT INTO child_table_schema_drop.orders_2023 (name, amount, purchase_date) VALUES
    ('John Doe', 100.00, '2023-01-01'),
    ('Jane Doe', 200.00, '2023-02-01'),
    ('Bob Doe', 300.00, '2023-03-01');

INSERT INTO child_table_schema_drop.orders_2024 (name, amount, purchase_date) VALUES
    ('Jerry Smith', 400.00, '2024-01-01'),
    ('Morty Smith', 500.00, '2024-02-01'),
    ('Beth Smith', 600.00, '2024-03-01');

DROP TABLE child_table_schema_drop.orders_2023;

## verify
### schema_check partition_table_schema_drop orders
### table_exists child_table_schema_drop orders_2023 false
### schema_check child_table_schema_drop orders_2024
SELECT * FROM partition_table_schema_drop.orders ORDER BY id;

## cleanup
DROP SCHEMA partition_table_schema_drop CASCADE;
