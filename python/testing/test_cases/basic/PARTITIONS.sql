## test
-- 1. Create the partitioned table
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL,
    name TEXT NOT NULL,
    department TEXT NOT NULL CHECK (department IN ('Sales', 'HR', 'IT')),
    PRIMARY KEY (department, id)
) PARTITION BY LIST (department);

-- 2. Create partitions
CREATE TABLE IF NOT EXISTS employees_sales PARTITION OF employees FOR VALUES IN ('Sales');
CREATE TABLE IF NOT EXISTS employees_hr PARTITION OF employees FOR VALUES IN ('HR');
CREATE TABLE IF NOT EXISTS employees_it PARTITION OF employees FOR VALUES IN ('IT');

-- 3. Insert sample data
INSERT INTO employees (name, department) VALUES
  ('Grace Lee', 'Sales'),
  ('Hank Miller', 'Sales'),
  ('Ivy Wilson', 'Sales'),
  ('Jack Davis', 'HR'),
  ('Kara Evans', 'HR'),
  ('Liam Clark', 'HR'),
  ('Mona Scott', 'IT'),
  ('Nate Hall', 'IT'),
  ('Olivia King', 'IT'),
  ('Paul Wright', 'Sales'),
  ('Quinn Baker', 'HR'),
  ('Rachel Adams', 'IT'),
  ('Steve Turner', 'Sales'),
  ('Tina Perez', 'HR'),
  ('Uma Foster', 'IT');


-- 1. Create the partitioned table
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL,
    sale_date DATE NOT NULL,
    amount NUMERIC NOT NULL,
    PRIMARY KEY (id, sale_date)
) PARTITION BY RANGE (sale_date);

-- 2. Partitions with correct non-overlapping ranges
CREATE TABLE IF NOT EXISTS sales_2022 PARTITION OF sales
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');

CREATE TABLE IF NOT EXISTS sales_2023 PARTITION OF sales
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE IF NOT EXISTS sales_2024 PARTITION OF sales
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS sales_2025 PARTITION OF sales
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- 3. Insert into sales
INSERT INTO sales (sale_date, amount) VALUES
  -- sales_2022 partition
  ('2022-01-10', 120.50),
  ('2022-03-15', 99.99),
  ('2022-06-30', 310.00),
  ('2022-09-05', 450.25),
  ('2022-12-20', 175.00),

  -- sales_2023 partition
  ('2023-01-01', 220.00),
  ('2023-04-18', 135.50),
  ('2023-07-22', 410.00),
  ('2023-10-30', 580.75),
  ('2023-12-31', 205.00),

  -- sales_2024 partition
  ('2024-02-14', 300.00),
  ('2024-05-01', 275.25),
  ('2024-08-19', 150.00),
  ('2024-11-11', 420.00),
  ('2024-12-30', 390.00),

  -- sales_2025 partition
  ('2025-01-05', 180.00),
  ('2025-03-15', 230.50),
  ('2025-06-25', 410.00),
  ('2025-09-10', 520.75),
  ('2025-12-31', 600.00);


-- 1. Create the partitioned table
CREATE TABLE IF NOT EXISTS sales_hash (
    id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    amount NUMERIC NOT NULL
) PARTITION BY HASH (id);

-- 2. Partitions by hash
CREATE TABLE IF NOT EXISTS sales_hash_p0 PARTITION OF sales_hash
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE IF NOT EXISTS sales_hash_p1 PARTITION OF sales_hash
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);

CREATE TABLE IF NOT EXISTS sales_hash_p2 PARTITION OF sales_hash
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);

CREATE TABLE IF NOT EXISTS sales_hash_p3 PARTITION OF sales_hash
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- 3. Insert into sales
INSERT INTO sales_hash (sale_date, amount) VALUES
  ('2023-01-05', 220.00),
  ('2023-01-06', 130.00),
  ('2023-01-07', 310.00),
  ('2023-01-08', 90.00),
  ('2023-01-09', 470.00),
  ('2023-01-10', 250.00),
  ('2023-01-11', 145.00),
  ('2023-01-12', 190.00),
  ('2023-01-13', 300.00),
  ('2023-01-14', 280.00),
  ('2023-01-15', 160.00),
  ('2023-01-16', 210.00),
  ('2023-01-17', 330.00),
  ('2023-01-18', 295.00),
  ('2023-01-19', 175.00),
  ('2023-01-20', 400.00),
  ('2023-01-21', 385.00),
  ('2023-01-22', 215.00),
  ('2023-01-23', 270.00),
  ('2023-01-24', 180.00);

-- 4. Create indices
CREATE INDEX idx_sales_hash_date ON sales_hash (sale_date);
CREATE INDEX idx_sales_hash_date_p0 ON sales_hash_p0 (sale_date);
CREATE INDEX idx_sales_hash_date_p1 ON sales_hash_p1 (sale_date);
CREATE INDEX idx_sales_hash_date_p2 ON sales_hash_p2 (sale_date);
CREATE INDEX idx_sales_hash_date_p3 ON sales_hash_p3 (sale_date);

-- Multi-level partitions

CREATE TABLE country_sales (
    id SERIAL,
    country TEXT NOT NULL,
    year INT NOT NULL,
    amount NUMERIC,
    PRIMARY KEY (country, year, id)
) PARTITION BY LIST (country);

CREATE TABLE country_sales_us PARTITION OF country_sales
    FOR VALUES IN ('US')
    PARTITION BY RANGE (year);

CREATE TABLE country_sales_us_2023 PARTITION OF country_sales_us
    FOR VALUES FROM (2023) TO (2024);

CREATE TABLE country_sales_us_2024 PARTITION OF country_sales_us
    FOR VALUES FROM (2024) TO (2025);

CREATE TABLE country_sales_ca PARTITION OF country_sales
    FOR VALUES IN ('CA')
    PARTITION BY RANGE (year);

CREATE TABLE country_sales_ca_2023 PARTITION OF country_sales_ca
    FOR VALUES FROM (2023) TO (2024);

CREATE TABLE country_sales_ca_2024 PARTITION OF country_sales_ca
    FOR VALUES FROM (2024) TO (2025);

INSERT INTO country_sales (country, year, amount)
SELECT
    country,
    year,
    ROUND((random() * 1000 + 100)::NUMERIC, 2)
FROM (
    SELECT unnest(ARRAY['US', 'CA']) AS country,
           unnest(ARRAY[2023, 2024]) AS year
) AS combos,
generate_series(1, 25);

## verify
-- verify
SELECT * FROM employees ORDER BY id;
SELECT * FROM sales ORDER BY id;
SELECT * FROM sales_hash ORDER BY id;

SELECT * FROM country_sales ORDER BY id;

## cleanup
-- cleanup
DROP TABLE employees CASCADE;
DROP TABLE sales CASCADE;
DROP TABLE sales_hash CASCADE;
DROP TABLE country_sales CASCADE;
