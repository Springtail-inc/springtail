## test
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL,
    name TEXT NOT NULL,
    department TEXT NOT NULL CHECK (department IN ('Sales', 'HR', 'IT')),
    salary NUMERIC NOT NULL,
    PRIMARY KEY (department, id)
);

INSERT INTO employees (name, department, salary) VALUES
  ('Grace Lee', 'Sales', 120.66),
  ('Hank Miller', 'Sales', 134.67),
  ('Ivy Wilson', 'Sales', 145.68),
  ('Jack Davis', 'HR', 156.69),
  ('Kara Evans', 'HR', 167.71),
  ('Liam Clark', 'HR', 178.72),
  ('Mona Scott', 'IT', 189.73),
  ('Nate Hall', 'IT', 191.74),
  ('Olivia King', 'IT', 202.75),
  ('Paul Wright', 'Sales', 213.76),
  ('Quinn Baker', 'HR', 224.77),
  ('Rachel Adams', 'IT', 235.78),
  ('Steve Turner', 'Sales', 246.79),
  ('Tina Perez', 'HR', 257.81),
  ('Uma Foster', 'IT', 268.82);

CREATE TABLE IF NOT EXISTS sales (
    id SERIAL,
    sale_date DATE NOT NULL,
    amount NUMERIC NOT NULL,
    PRIMARY KEY (id, sale_date)
);

INSERT INTO sales (sale_date, amount) VALUES
  ('2022-01-10', 120.50),
  ('2022-03-15', 99.99),
  ('2022-06-30', 310.00),
  ('2022-09-05', 450.25),
  ('2022-12-20', 175.00),
  ('2023-01-01', 220.00),
  ('2023-04-18', 135.50),
  ('2023-07-22', 410.00),
  ('2023-10-30', 580.75),
  ('2023-12-31', 205.00),
  ('2024-02-14', 300.00),
  ('2024-05-01', 275.25),
  ('2024-08-19', 150.00),
  ('2024-11-11', 420.00),
  ('2024-12-30', 390.00),
  ('2025-01-05', 180.00),
  ('2025-03-15', 230.50),
  ('2025-06-25', 410.00),
  ('2025-09-10', 520.75),
  ('2025-12-31', 600.00);


CREATE TABLE IF NOT EXISTS sales_hash (
    id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    amount NUMERIC NOT NULL
);

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

CREATE INDEX idx_sales_hash_date ON sales_hash (sale_date);
CREATE INDEX idx_sales_hash_amount ON sales_hash (amount);

CREATE TABLE country_sales (
    id SERIAL,
    country TEXT NOT NULL,
    year INT NOT NULL,
    amount NUMERIC(6, 2),
    PRIMARY KEY (country, year, id)
);

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

-- test NUMERIC as primary key
CREATE TABLE measurements (
    reading NUMERIC PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT now()
);

INSERT INTO measurements (reading, sensor_id)
SELECT
    (i / 1000000.0)::NUMERIC,
    'sensor_' || (i % 10)
FROM generate_series(1, 10) AS s(i);

-- test NUMERIC with composite index
CREATE TABLE coordinates (
    x NUMERIC,
    y NUMERIC
);

CREATE INDEX coordinates_xy_idx ON coordinates (x, y);

INSERT INTO coordinates (x, y)
SELECT
    ROUND((random() * 1000)::NUMERIC, 4),
    ROUND((random() * 500)::NUMERIC, 4)
FROM generate_series(1, 100);


## verify
-- verify
SELECT * FROM employees ORDER BY id;
SELECT * FROM sales ORDER BY id;
SELECT * FROM sales_hash ORDER BY id;
SELECT * FROM sales_hash ORDER BY amount ASC;
SELECT * FROM sales_hash ORDER BY amount DESC;

SELECT * FROM sales_hash WHERE amount = 330.00;
SELECT * FROM sales_hash WHERE amount < 330.00 ORDER BY amount ASC;
SELECT * FROM sales_hash WHERE amount > 330.00 ORDER BY amount ASC;
SELECT * FROM sales_hash WHERE amount < 330.00 ORDER BY amount DESC;
SELECT * FROM sales_hash WHERE amount > 330.00 ORDER BY amount DESC;

SELECT * FROM country_sales ORDER BY id;

SELECT * FROM measurements ORDER BY reading;

SELECT * FROM coordinates ORDER BY x ASC, y ASC;
SELECT * FROM coordinates ORDER BY x ASC, y DESC;
SELECT * FROM coordinates ORDER BY x DESC, y ASC;
SELECT * FROM coordinates ORDER BY x DESC, y DESC;

## cleanup
-- cleanup
DROP TABLE employees CASCADE;
DROP TABLE sales CASCADE;
DROP TABLE sales_hash CASCADE;
DROP TABLE country_sales CASCADE;
DROP TABLE measurements CASCADE;
