CREATE TABLE IF NOT EXISTS customers (
    CustomerID SERIAL PRIMARY KEY,
    FirstName TEXT,
    MiddleInitial TEXT,
    LastName TEXT,
    CityID INT,
    Address TEXT
);

CREATE TABLE IF NOT EXISTS sales (
    SalesID SERIAL PRIMARY KEY,
    SalesPersonID INT,
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    Discount REAL,
    TotalPrice REAL,
    SalesDate TIMESTAMP,
    TransactionNumber TEXT
);
