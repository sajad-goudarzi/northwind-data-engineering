-- Total Revenue --
select sum(quantity*unit_price*(1-discount)) as "Total Revenue" from northwind.order_details
