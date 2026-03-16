-- models/staging/stg_order_items.sql
with source as (
    select * from {{ ref('raw_order_items') }}
),

cleaned as (
    select
        order_item_id,
        order_id,
        product_id,
        cast(quantity as integer)           as quantity,
        cast(unit_price as numeric)         as unit_price,
        cast(unit_cost as numeric)          as unit_cost,
        cast(discount_amount as numeric)    as discount_amount,
        cast(line_total as numeric)         as line_total,
        cast(line_profit as numeric)        as line_profit,

        -- Derived
        cast(line_profit as numeric)
            / nullif(cast(line_total as numeric), 0)    as margin_pct,
        cast(unit_price as numeric)
            - cast(unit_cost as numeric)                as unit_margin

    from source
    where order_item_id is not null
      and order_id is not null
      and product_id is not null
      and cast(quantity as integer) > 0
)

select * from cleaned
