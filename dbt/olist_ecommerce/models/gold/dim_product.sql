WITH serogated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY product_id) as product_sk
    FROM {{ ref('silver_products') }}
)
SELECT 
    product_sk,
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_width_cm
FROM serogated
