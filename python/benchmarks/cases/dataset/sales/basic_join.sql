DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('select * from customers join sales on customers.CustomerID=sales.CustomerID') INTO v;
END;
$FN$;
