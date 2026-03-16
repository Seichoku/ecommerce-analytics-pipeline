-- models/marts/mart_product_performance.sql
-- Product-level performance mart.
-- Supports category analysis, top/bottom performer identification,
-- and margin optimization reporting.

with products as (
    select * from {{ ref('raw_products') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, order_date, order_month, order_year,
           order_status, is_completed, country, region
    from {{ ref('fct_orders') }}
),

items_joined as (
    select
        i.*,
        o.order_date,
        o.order_month,
        o.order_year,
        o.is_completed,
        o.country,
        o.region
    from order_items i
    inner join orders o on i.order_id = o.order_id
),

product_agg as (
    select
        product_id,

        -- Volume
        count(distinct order_id)                        as total_orders,
        sum(quantity)                                   as total_units_sold,
        count(*) filter (where is_completed)            as completed_line_items,

        -- Revenue
        sum(line_total) filter (where is_completed)     as total_revenue,
        sum(line_profit) filter (where is_completed)    as total_profit,
        avg(line_total)                                 as avg_line_value,
        avg(margin_pct) filter (where is_completed)     as avg_margin_pct,

        -- Time
        min(order_date)                                 as first_sale_date,
        max(order_date)                                 as last_sale_date

    from items_joined
    group by product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.cost_price,
        p.selling_price,
        p.is_active,

        -- Performance
        coalesce(a.total_orders, 0)         as total_orders,
        coalesce(a.total_units_sold, 0)     as total_units_sold,
        coalesce(a.total_revenue, 0)        as total_revenue,
        coalesce(a.total_profit, 0)         as total_profit,
        coalesce(a.avg_line_value, 0)       as avg_line_value,
        coalesce(a.avg_margin_pct, 0)       as avg_margin_pct,
        a.first_sale_date,
        a.last_sale_date,

        -- Rank within category
        rank() over (
            partition by p.category
            order by coalesce(a.total_revenue, 0) desc
        )                                   as revenue_rank_in_category,

        -- Performance tier
        case
            when coalesce(a.total_revenue, 0) >= 200000 then 'Top Performer'
            when coalesce(a.total_revenue, 0) >= 80000  then 'Strong'
            when coalesce(a.total_revenue, 0) >= 20000  then 'Average'
            when coalesce(a.total_revenue, 0) > 0       then 'Under-performer'
            else 'No Sales'
        end                                 as performance_tier

    from products p
    left join product_agg a on p.product_id = a.product_id
)

select * from final
order by total_revenue desc
