WITH dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY 1) as rn
    FROM {{ source('bronze', 'customers') }}
)
SELECT 
    customer_id,
    customer_unique_id,
    cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
    customer_city,
    customer_state
FROM dedup 
WHERE 
    rn = 1 AND
    customer_id IS NOT NULL
