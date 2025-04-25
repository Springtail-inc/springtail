DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('select * from sales where CustomerID > 200') INTO v;
END;
$FN$;
