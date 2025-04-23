select ProductID, SUM(Quantity) as quantity from sales group by productID order by quantity DESC;
