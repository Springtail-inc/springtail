## test
CREATE SCHEMA partition_table_schema;

CREATE SCHEMA child_table_schema;

CREATE TABLE partition_table_schema.invoice (
    id SERIAL,
    name TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    purchase_date DATE NOT NULL,
    PRIMARY KEY (id, purchase_date)
) PARTITION BY RANGE (purchase_date);

CREATE TABLE child_table_schema.invoice_2023 PARTITION OF partition_table_schema.invoice
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

INSERT INTO partition_table_schema.invoice (name, amount, purchase_date) VALUES
    ('John Doe', 100.00, '2023-01-01'),
    ('Jane Doe', 200.00, '2023-02-01'),
    ('Bob Smith', 300.00, '2023-03-01');

## verify
SELECT * FROM partition_table_schema.invoice ORDER BY id;

## cleanup
DROP SCHEMA partition_table_schema CASCADE;
