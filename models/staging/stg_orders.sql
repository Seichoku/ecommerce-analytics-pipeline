-- models/staging/stg_orders.sql
-- Staging layer: clean and type-cast raw orders source data.
-- Applies basic business rules before marts consumption.

with source as (
    select * from {{ ref('raw_orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        cast(order_date as date)                        as order_date,
        order_status,
        channel,
        cast(discount_pct as numeric)                   as discount_pct,
        cast(shipping_days as integer)                  as shipping_days,
        cast(is_first_order as boolean)                 as is_first_order,

        -- Derived flags
        case
            when order_status = 'completed' then true
            else false
        end                                             as is_completed,

        case
            when order_status = 'returned' then true
            else false
        end                                             as is_returned,

        -- Date parts for easy partitioning
        date_trunc('month', cast(order_date as date))   as order_month,
        date_part('year',   cast(order_date as date))   as order_year,
        date_part('quarter',cast(order_date as date))   as order_quarter,
        date_part('dow',    cast(order_date as date))   as day_of_week

    from source
    where order_id is not null
      and customer_id is not null
      and order_date is not null
)

select * from cleaned
