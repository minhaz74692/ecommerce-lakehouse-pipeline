WITH source AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    FROM {{ source('bronze', 'payments') }}
    WHERE order_id IS NOT NULL
)
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    ROUND(SUM(payment_value) OVER (PARTITION BY order_id), 2) AS order_total_payment
FROM source
