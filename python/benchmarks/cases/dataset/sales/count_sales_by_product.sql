DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('select ProductID, SUM(Quantity) as quantity from sales group by productID order by quantity DESC') INTO v;
END;
$FN$;
