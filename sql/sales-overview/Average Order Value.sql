-- Average Order Value --
SELECT
    sum(unit_price * quantity * (1 - discount)) / uniqExact(order_id) AS avg_order_value
FROM northwind.order_details
