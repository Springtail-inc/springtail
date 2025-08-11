## metadata
### autocommit true

## setup

-- load a large CSV file
CREATE TABLE IF NOT EXISTS customers (
    CustomerID SERIAL PRIMARY KEY,
    FirstName TEXT,
    MiddleInitial TEXT,
    LastName TEXT,
    CityID INT,
    Address TEXT
);
CREATE INDEX idx_lastname ON customers (LastName);

### load_csv customers.csv customers

## cleanup
DROP TABLE customers;
