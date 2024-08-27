WITH order_data AS (
    SELECT
        orders_id,
        date_date,
        SUM(revenue) AS revenue,  -- Suma el revenue por order_id
        SUM(quantity) AS quantity,  -- Suma la cantidad por order_id
        SUM(purchase_cost) AS purchase_cost,  -- Suma el costo de compra por order_id
        SUM(margin) AS margin  -- Suma el margen por order_id
    FROM
        {{ ref('int_sales_margin') }}  -- Referencia el modelo anterior
    GROUP BY
        orders_id, date_date  -- Agrupa por order_id y fecha
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
