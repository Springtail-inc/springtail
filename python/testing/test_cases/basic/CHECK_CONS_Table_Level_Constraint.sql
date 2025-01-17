## test
CREATE TABLE IF NOT EXISTS table_with_table_constraint (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    stock INT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_price_stock CHECK (price > 0 AND stock >= 0)
);

-- Insert valid rows
INSERT INTO table_with_table_constraint (product_name, product_category, stock, price)
VALUES 
('Laptop', 'Electronics', 10, 999.99),
('Phone', 'Electronics', 20, 799.99),
('Desk', 'Furniture', 5, 149.50);

## verify
### schema_check public table_with_table_constraint
SELECT * FROM table_with_table_constraint;

## cleanup
DROP TABLE IF EXISTS table_with_table_constraint;

