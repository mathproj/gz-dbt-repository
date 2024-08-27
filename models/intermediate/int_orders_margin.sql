WITH order_data AS (
    SELECT
        orders_id,
        date_date,
        ROUND(SUM(revenue),2) AS revenue,  -- Suma el revenue por order_id
        ROUND(SUM(quantity),2) AS quantity,  -- Suma la cantidad por order_id
        ROUND(SUM(purchase_cost),2) AS purchase_cost,  -- Suma el costo de compra por order_id
        ROUND(SUM(margin),2) AS margin  -- Suma el margen por order_id
    FROM
        {{ ref('int_sales_margin') }}  -- Referencia el modelo anterior
    GROUP BY
        orders_id, date_date  -- Agrupa por order_id y fecha
    ORDER BY orders_id DESC, date_date DESC
)

SELECT
    orders_id,
    date_date,
    revenue,
    quantity,
    purchase_cost,
    margin
FROM
    order_data
