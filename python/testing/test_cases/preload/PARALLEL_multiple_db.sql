## test

### parallel

### txn 1
### switch_db parallel_db1

-- Create parent table
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
);

-- Create child table with foreign key
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    total NUMERIC(10, 2) NOT NULL
);

### txn 2
### switch_db parallel_db2

-- Create parent table
CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- Create child table with foreign key
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category_id INTEGER NOT NULL REFERENCES categories(category_id),
    price NUMERIC(10, 2)
);

### sequential

### parallel

### txn 1
### switch_db parallel_db2
INSERT INTO categories (name)
SELECT 'Category_' || gs FROM generate_series(1, 10) AS gs;

INSERT INTO products (name, category_id, price)
SELECT
    'Product_' || gs,
    (random() * 9 + 1)::int,
    round((random() * 500 + 1)::numeric, 2)
FROM generate_series(1, 100) AS gs;

### txn 2
### switch_db parallel_db1
INSERT INTO users (name, email)
SELECT
    'User_' || gs,
    'user_' || gs || '@example.com'
FROM generate_series(1, 100) AS gs;

INSERT INTO orders (user_id, total)
SELECT
    (random() * 99 + 1)::int,
    round((random() * 1000)::numeric, 2)
FROM generate_series(1, 200);

## verify

### switch_db parallel_db1
SELECT * FROM users ORDER BY user_id;
SELECT * FROM orders ORDER BY order_id;

### switch_db parallel_db2
SELECT * FROM categories ORDER BY category_id;
SELECT * FROM products ORDER BY product_id;

## cleanup

### switch_db parallel_db1
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS users CASCADE;

### switch_db parallel_db2
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS categories CASCADE;



