DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('select * from sales where customerID=20') INTO v;
END;
$FN$;
