## metadata
### autocommit true

## test

## verify
### benchmark 100 5000 "SELECT * FROM customers"
### benchmark 50 2000 "SELECT CustomerID FROM customers"
### benchmark 70 2000 "SELECT LastName FROM customers"
