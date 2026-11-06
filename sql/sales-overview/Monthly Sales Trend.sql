-- Monthly Sales Trend --
select toStartOfMonth(toDate(o.order_date)) AS time,sum(od.unit_price*od.quantity*(1-od.discount)) as revenue from northwind.order_details as od
join northwind.orders as o
on o.order_id=od.order_id
group by toStartOfMonth(toDate(o.order_date))
order by toStartOfMonth(toDate(o.order_date))
