-- Top 10 Product by Revenue --
select toString(product_id) as Product,sum(quantity*unit_price*(1-discount)) as Revenue from northwind.order_details
group by product_id
order by Revenue desc
limit 10
