WITH operational_data AS (
    SELECT
        o.orders_id,
        o.date_date,
        o.margin,  -- Selecciona el margen calculado en el modelo anterior
        o.quantity,
        o.revenue,
        o.purchase_cost,
        s.shipping_fee,
        s.logcost,
        CAST(s.ship_cost AS FLOAT64) AS ship_cost,  -- Asegúrate de convertir ship_cost a un formato numérico
        ROUND(
            o.margin + s.shipping_fee - s.logcost - CAST(s.ship_cost AS FLOAT64), 
            2
        ) AS operational_margin  -- Calcular el margen operativo
    FROM
        {{ ref('int_orders_margin') }} o  -- Refiere el modelo de margen por orden
    JOIN
        {{ ref('stg_raw__ship') }} s  -- Refiere el modelo de shipping
    ON
        o.orders_id = s.orders_id  -- Une las tablas sobre el id de la orden
    ORDER BY 
        o.orders_id DESC, 
        o.date_date DESC
)

SELECT
    orders_id,
    date_date,
    operational_margin,  -- Muestra el margen operativo calculado
    revenue,
    margin,
    shipping_fee,
    logcost,
    ship_cost
FROM
    operational_data

