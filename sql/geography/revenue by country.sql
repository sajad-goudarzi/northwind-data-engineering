-- revenue by country --
select o.ship_country as country,sum(od.unit_price*od.quantity*(1-od.discount)) as Revenue from northwind.order_details as od
join northwind.orders as o
on od.order_id=o.order_id
group by o.ship_country
ORDER BY Revenue DESC
