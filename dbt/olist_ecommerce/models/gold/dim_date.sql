with bounds as (

    select
        min(d) as global_min,
        max(d) as global_max
    from (
        select to_date(shipping_limit_date)           as d from {{ ref('silver_order_items') }}
        union all
        select to_date(order_purchase_timestamp)      as d from {{ ref('silver_orders') }}
        union all
        select to_date(order_approved_at)             as d from {{ ref('silver_orders') }}
        union all
        select to_date(order_delivered_carrier_date)  as d from {{ ref('silver_orders') }}
        union all
        select to_date(order_delivered_customer_date) as d from {{ ref('silver_orders') }}
        union all
        select to_date(order_estimated_delivery_date) as d from {{ ref('silver_orders') }}
        union all
        select to_date(review_creation_date)          as d from {{ ref('silver_reviews') }}
        union all
        select to_date(review_answer_timestamp)       as d from {{ ref('silver_reviews') }}
    )
    where d is not null

),

padded as (

    select
        trunc(global_min, 'YEAR')                                as start_date,
        last_day(add_months(trunc(global_max, 'YEAR'), 11))      as end_date
    from bounds

),

spine as (

    select explode(sequence(start_date, end_date, interval 1 day)) as date
    from padded

)

select
    cast(date_format(date, 'yyyyMMdd') as int)  as date_key,
    date                                        as date,
    year(date)                                  as year,
    quarter(date)                               as quarter,
    month(date)                                 as month,
    date_format(date, 'MMMM')                   as month_name,
    dayofmonth(date)                            as day,
    dayofweek(date)                             as day_of_week,
    date_format(date, 'EEEE')                   as day_name,
    dayofyear(date)                             as day_of_year,
    weekofyear(date)                            as week_of_year,
    dayofweek(date) in (1, 7)                   as is_weekend,
    date_format(date, 'yyyy-MM')                as year_month,
    concat('Q', quarter(date))                  as quarter_name,
    trunc(date, 'MONTH')                        as first_day_of_month,
    last_day(date)                              as last_day_of_month
from spine