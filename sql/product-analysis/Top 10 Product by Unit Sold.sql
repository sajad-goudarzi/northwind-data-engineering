-- Top 10 Product by Unit Sold --
select toString(product_id) as "Product ID" ,sum(quantity) as "Units Sold" from northwind.order_details
group by product_id
order by sum(quantity) desc
limit 10
