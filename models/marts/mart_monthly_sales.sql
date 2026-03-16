-- models/marts/mart_monthly_sales.sql
-- Monthly sales performance mart.
-- Powers executive dashboards: revenue, profit, AOV, orders, retention.

with base as (
    select * from {{ ref('fct_orders') }}
    where is_completed = true
),

monthly as (
    select
        order_month,
        order_year,
        order_quarter,

        -- Volume
        count(distinct order_id)            as total_orders,
        count(distinct customer_id)         as unique_customers,
        sum(total_units)                    as total_units_sold,

        -- Revenue & Profit
        sum(net_revenue)                    as total_revenue,
        sum(net_profit)                     as total_profit,
        sum(total_discount)                 as total_discounts_given,
        avg(gross_revenue)                  as avg_order_value,
        avg(avg_margin_pct)                 as avg_margin_pct,

        -- Customer mix
        count(*) filter (where is_first_order)              as new_customers,
        count(*) filter (where not is_first_order)          as returning_customers,
        count(*) filter (where is_loyalty_member)           as loyalty_orders,

        -- Channel breakdown
        count(*) filter (where order_channel = 'organic_search')    as organic_search_orders,
        count(*) filter (where order_channel = 'paid_search')       as paid_search_orders,
        count(*) filter (where order_channel = 'email')             as email_orders,
        count(*) filter (where order_channel = 'social_media')      as social_orders,
        count(*) filter (where order_channel = 'direct')            as direct_orders,
        count(*) filter (where order_channel = 'referral')          as referral_orders

    from base
    group by order_month, order_year, order_quarter
),

with_growth as (
    select
        *,
        -- MoM revenue growth
        total_revenue - lag(total_revenue) over (order by order_month)  as revenue_mom_delta,
        round(
            100.0 * (total_revenue - lag(total_revenue) over (order by order_month))
            / nullif(lag(total_revenue) over (order by order_month), 0),
        2)                                                              as revenue_mom_pct,

        -- Repeat customer rate
        round(
            100.0 * returning_customers / nullif(total_orders, 0),
        2)                                                              as repeat_customer_rate,

        -- New customer rate
        round(
            100.0 * new_customers / nullif(total_orders, 0),
        2)                                                              as new_customer_rate,

        -- Cumulative revenue (running total)
        sum(total_revenue) over (
            partition by order_year
            order by order_month
            rows between unbounded preceding and current row
        )                                                               as ytd_revenue

    from monthly
)

select * from with_growth
order by order_month
