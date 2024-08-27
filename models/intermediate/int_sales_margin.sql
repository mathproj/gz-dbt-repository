WITH sales_data AS (
    SELECT 
        s.orders_id,
        s.date_date,
        s.quantity,
        s.revenue,
        p.products_id,
        p.purchase_price  -- Usa el nombre de columna correcta para el precio de compra
    FROM 
        {{ ref('stg_raw__sales') }} s  -- Refiere el modelo de ventas
    JOIN 
        {{ ref('stg_raw__product') }} p  -- Refiere el modelo de productos
    ON 
        s.products_id = p.products_id  -- Une las tablas usando el id del producto
)

SELECT 
    sales_data.*,
    (ROUND(sales_data.quantity * CAST(sales_data.purchase_price AS FLOAT64),2)) AS purchase_cost,  -- Calcular purchase_cost
    (ROUND(sales_data.revenue - (sales_data.quantity * CAST(sales_data.purchase_price AS FLOAT64)),2)) AS margin  -- Calcular margin
FROM 
    sales_data
