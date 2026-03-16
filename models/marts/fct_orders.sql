-- models/marts/fct_orders.sql
-- Core orders fact table joining orders, customers, and aggregated line items.
-- This is the primary mart for sales reporting and KPI dashboards.

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

order_items_agg as (
    select
        order_id,
        count(*)                        as total_line_items,
        sum(quantity)                   as total_units,
        sum(line_total)                 as gross_revenue,
        sum(line_profit)                as gross_profit,
        sum(discount_amount * quantity) as total_discount,
        avg(margin_pct)                 as avg_margin_pct
    from {{ ref('stg_order_items') }}
    group by order_id
),

final as (
    select
        -- Keys
        o.order_id,
        o.customer_id,

        -- Customer context
        c.full_name          as customer_name,
        c.country,
        c.region,
        c.acquisition_channel as customer_channel,
        c.is_loyalty_member,
        c.age_group,
        c.cohort_month,

        -- Order details
        o.order_date,
        o.order_month,
        o.order_year,
        o.order_quarter,
        o.day_of_week,
        o.order_status,
        o.channel            as order_channel,
        o.discount_pct,
        o.shipping_days,
        o.is_first_order,
        o.is_completed,
        o.is_returned,

        -- Financials
        i.total_line_items,
        i.total_units,
        i.gross_revenue,
        i.gross_profit,
        i.total_discount,
        i.avg_margin_pct,

        -- Derived metrics
        case
            when o.is_completed then i.gross_revenue
            else 0
        end                             as net_revenue,

        case
            when o.is_completed then i.gross_profit
            else 0
        end                             as net_profit,

        i.gross_revenue
            / nullif(i.total_units, 0)  as avg_order_value_per_unit

    from orders o
    left join customers c   on o.customer_id  = c.customer_id
    left join order_items_agg i on o.order_id = i.order_id
)

select * from final
