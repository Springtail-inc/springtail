## test
CREATE TABLE expand_parent_table (
    id SERIAL,
    order_id INT,
    order_date DATE,
    order_amount NUMERIC,
    PRIMARY KEY (id, order_date)
) PARTITION BY RANGE (order_date);

CREATE TABLE expand_child_table_001_2023 PARTITION OF expand_parent_table
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE expand_child_table_001_2024 PARTITION OF expand_parent_table
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE expand_child_table_001_2025 PARTITION OF expand_parent_table
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

INSERT INTO expand_child_table_001_2023 (order_id, order_date, order_amount) VALUES
    (1001, '2023-01-01', 100.00),
    (1002, '2023-02-01', 200.00),
    (1003, '2023-03-01', 300.00);

INSERT INTO expand_child_table_001_2024 (order_id, order_date, order_amount) VALUES
    (2002, '2024-01-01', 200.00),
    (2003, '2024-02-01', 300.00),
    (2004, '2024-03-01', 400.00);

INSERT INTO expand_child_table_001_2025 (order_id, order_date, order_amount) VALUES
    (3003, '2025-01-01', 300.00),
    (3004, '2025-02-01', 400.00),
    (3005, '2025-03-01', 500.00);

CREATE TABLE expand_child_table_001_2024_2025 (
    id SERIAL,
    order_id INT,
    order_date DATE,
    order_amount NUMERIC,
    PRIMARY KEY (id, order_date)
);

INSERT INTO expand_child_table_001_2024_2025 SELECT * FROM expand_child_table_001_2024;
INSERT INTO expand_child_table_001_2024_2025 SELECT * FROM expand_child_table_001_2025;

ALTER TABLE expand_parent_table DETACH PARTITION expand_child_table_001_2024;
ALTER TABLE expand_parent_table DETACH PARTITION expand_child_table_001_2025;
ALTER TABLE expand_parent_table ATTACH PARTITION expand_child_table_001_2024_2025 FOR VALUES FROM ('2024-01-01') TO ('2026-01-01');

## verify
### schema_check public expand_parent_table
### schema_check public expand_child_table_001_2024_2025
SELECT * FROM expand_parent_table ORDER BY order_id, order_date;
SELECT * FROM expand_child_table_001_2024_2025 ORDER BY order_id, order_date;

## cleanup
DROP TABLE expand_parent_table CASCADE;
