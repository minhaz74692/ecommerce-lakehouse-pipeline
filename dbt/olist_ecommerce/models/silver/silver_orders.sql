WITH dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_purchase_timestamp DESC, _load_timestamp DESC) as rn
    FROM {{ source('bronze', 'orders') }}
)
SELECT 
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM dedup
WHERE 
    rn = 1 AND
    customer_id IS NOT NULL AND
    order_id IS NOT NULL
