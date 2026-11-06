-- Customers By Country --
select uniqExact(customer_id) as Customers,ship_country as Country from northwind.orders
group by ship_country
order by Customers desc
