{{ config(materialized='view') }}

with joined as (
    select
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.order_purchase_timestamp
    from {{ ref('silver_customers') }} c
    left join {{ ref('silver_orders') }} o
        on c.customer_id = o.customer_id
)

select
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from joined
qualify row_number() over (
    partition by customer_unique_id
    order by order_purchase_timestamp desc
) = 1