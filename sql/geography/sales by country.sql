-- sales by country --
select o.ship_country as Country,sum(od.quantity) from northwind.order_details as od
join northwind.orders as o
on o.order_id=od.order_id
group by o.ship_country
