-- Total Revenue --
select sum(od.quantity*od.unit_price*(1-od.discount)) as Total_Revenue
from northwind.order_details as od
join northwind.orders as o
on o.order_id=od.order_id
