DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('SELECT s.salesid, s.salesdate, s.productid, s.quantity, s.totalprice, c.customerid, c.firstname, c.lastname, c.cityid FROM public.sales     s JOIN public.customers c ON c.customerid = s.customerid WHERE s.salesid = 2567') INTO v;
END;
$FN$;
