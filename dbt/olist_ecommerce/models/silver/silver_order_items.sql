WITH dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id, order_item_id ORDER BY shipping_limit_date DESC, _load_timestamp DESC) as rn
    FROM {{ source('bronze', 'order_items') }}
)
SELECT 
    order_id,
    order_item_id,	
    product_id,
    seller_id,
    shipping_limit_date,
    CAST(price AS DECIMAL(10,2)) AS price,
    CAST(freight_value AS DECIMAL(10,2)) AS freight_value
FROM dedup
WHERE 
    rn = 1 AND
    order_id IS NOT NULL AND
    order_item_id IS NOT NULL


