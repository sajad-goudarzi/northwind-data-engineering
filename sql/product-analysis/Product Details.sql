-- Product Details --
select product_id as "Product ID" ,sum(quantity) as "Unit Sold",sum(quantity*unit_price*(1-discount)) as "Total Revenue",avg(unit_price) "Average Unit Price",sum(quantity*unit_price*discount) "Total discount" from northwind.order_details
group by product_id
order by product_id
