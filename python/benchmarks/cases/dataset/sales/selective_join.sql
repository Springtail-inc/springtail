DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('select * from customers, sales where customers.CustomerID=sales.CustomerID and customers.customerID=20') INTO v;
END;
$FN$;
