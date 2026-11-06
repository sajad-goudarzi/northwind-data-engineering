-- Monthly Order Volume --
SELECT
    toStartOfMonth(toDate(o.order_date)) AS month,
    uniqExact(o.order_id) AS order_volume
FROM northwind.orders AS o
GROUP BY month
ORDER BY month;
