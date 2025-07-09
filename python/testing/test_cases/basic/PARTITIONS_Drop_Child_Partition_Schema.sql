## test
CREATE SCHEMA partition_schema_drop_child_schema;

CREATE SCHEMA child_schema_to_drop_1;
CREATE SCHEMA child_schema_to_drop_2;

CREATE TABLE partition_schema_drop_child_schema.invoice (
    id SERIAL,
    name TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    purchase_date DATE NOT NULL,
    PRIMARY KEY (id, purchase_date)
) PARTITION BY RANGE (purchase_date);

CREATE TABLE child_schema_to_drop_1.invoice_2023 PARTITION OF partition_schema_drop_child_schema.invoice
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE child_schema_to_drop_2.invoice_2024 PARTITION OF partition_schema_drop_child_schema.invoice
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

INSERT INTO child_schema_to_drop_1.invoice_2023 (name, amount, purchase_date) VALUES
    ('John Doe', 100.00, '2023-01-01'),
    ('Jane Doe', 200.00, '2023-02-01'),
    ('Bob Smith', 300.00, '2023-03-01');

INSERT INTO child_schema_to_drop_2.invoice_2024 (name, amount, purchase_date) VALUES
    ('John Doe', 100.00, '2024-01-01'),
    ('Jane Doe', 200.00, '2024-02-01'),
    ('Bob Smith', 300.00, '2024-03-01');

DROP SCHEMA child_schema_to_drop_1 CASCADE;

## verify
### schema_check partition_schema_drop_child_schema invoice
SELECT * FROM partition_schema_drop_child_schema.invoice ORDER BY id;

## cleanup
DROP SCHEMA partition_schema_drop_child_schema CASCADE;
