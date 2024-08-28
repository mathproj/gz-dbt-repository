SELECT
    o.date_date
    ,COUNT(o.orders_id) AS nb_transactions
    ,ROUND(SUM(o.revenue),0) AS revenue 
    ,ROUND(AVG(o.revenue),1) AS average_basket
    ,ROUND(SUM(o.revenue)/COUNT(orders_id),1) AS average_basket_bis
    ,ROUND(SUM(o.margin),0) AS margin 
    ,ROUND(SUM(o.operational_margin),0) AS operational_margin 
    ,ROUND(SUM(o.purchase_cost),0) AS purchase_cost 
    ,ROUND(SUM(o.shipping_fee),0) AS shipping_fee 
    ,ROUND(SUM(o.logcost),0) AS logcost 
    ,ROUND(SUM(o.ship_cost),0) AS ship_cost 
    ,SUM(o.quantity) AS quantity 
FROM {{ref("int_orders_operational")}} o
GROUP BY  date_date
ORDER BY  date_date DESC