## metadata
### sync_timeout 20

## test
CREATE TABLE IF NOT EXISTS parallel_trans_add_secondary_index (
    id SERIAL PRIMARY KEY,
    item_name TEXT,
    amount DECIMAL(10, 2),
    category TEXT
);

### parallel
### txn 1
INSERT INTO parallel_trans_add_secondary_index (item_name, amount, category)
SELECT 'Item-' || i, (RANDOM() * 1000)::INT, 'Category-' || i
FROM generate_series(1, 100000) AS i;

### txn 2
### sleep 2
CREATE INDEX idx_amount ON parallel_trans_add_secondary_index (amount);
### sleep 4
CREATE INDEX idx_item_name_category ON parallel_trans_add_secondary_index (item_name, category);

## verify
### schema_check public parallel_trans_add_secondary_index
SELECT * FROM parallel_trans_add_secondary_index;

## cleanup
DROP TABLE IF EXISTS parallel_trans_add_secondary_index;
