WITH dedup AS (
    SELECT 
        * ,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY 1) as rn
    FROM {{ source('bronze', 'products') }}
)
SELECT 
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght, 
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_width_cm
FROM dedup
WHERE rn =1