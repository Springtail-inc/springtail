DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('
select sales.customerid, customers.firstname, customers.lastname, count(*) as sales from sales join customers on sales.customerid=customers.customerid group by sales.CustomerID, customers.firstname, customers.lastname') INTO v;
END;
$FN$;
