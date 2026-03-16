-- models/marts/dim_customers.sql
-- Customer dimension enriched with lifetime value, order history,
-- and RFM (Recency, Frequency, Monetary) segmentation.
-- Powers customer analytics, cohort analysis, and loyalty reporting.

with customers as (
    select * from {{ ref('stg_customers') }}
),

order_history as (
    select
        customer_id,
        count(*)                                    as total_orders,
        count(*) filter (where is_completed)        as completed_orders,
        count(*) filter (where is_returned)         as returned_orders,
        sum(net_revenue)                            as lifetime_revenue,
        sum(net_profit)                             as lifetime_profit,
        avg(gross_revenue)                          as avg_order_value,
        min(order_date)                             as first_order_date,
        max(order_date)                             as last_order_date,
        max(order_date)                             as most_recent_order_date
    from {{ ref('fct_orders') }}
    group by customer_id
),

rfm_scores as (
    select
        customer_id,

        -- Recency: days since last order (lower = better)
        current_date - most_recent_order_date       as recency_days,

        -- Frequency score 1-5
        case
            when completed_orders >= 10 then 5
            when completed_orders >= 6  then 4
            when completed_orders >= 3  then 3
            when completed_orders >= 2  then 2
            else 1
        end                                         as frequency_score,

        -- Monetary score 1-5
        case
            when lifetime_revenue >= 5000 then 5
            when lifetime_revenue >= 2000 then 4
            when lifetime_revenue >= 800  then 3
            when lifetime_revenue >= 300  then 2
            else 1
        end                                         as monetary_score,

        -- Recency score 1-5
        case
            when current_date - most_recent_order_date <= 30  then 5
            when current_date - most_recent_order_date <= 90  then 4
            when current_date - most_recent_order_date <= 180 then 3
            when current_date - most_recent_order_date <= 365 then 2
            else 1
        end                                         as recency_score

    from order_history
),

rfm_segments as (
    select
        customer_id,
        recency_days,
        recency_score,
        frequency_score,
        monetary_score,
        recency_score + frequency_score + monetary_score    as rfm_total,

        case
            when recency_score >= 4 and frequency_score >= 4 then 'Champions'
            when recency_score >= 3 and frequency_score >= 3 then 'Loyal Customers'
            when recency_score >= 4 and frequency_score <= 2 then 'Recent Customers'
            when recency_score <= 2 and frequency_score >= 3 then 'At Risk'
            when recency_score <= 2 and frequency_score <= 2 then 'Lost'
            else 'Potential Loyalists'
        end                                                 as rfm_segment

    from rfm_scores
),

final as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.country,
        c.region,
        c.acquisition_channel,
        c.registration_date,
        c.cohort_month,
        c.registration_year,
        c.is_loyalty_member,
        c.age_group,

        -- Order history
        coalesce(h.total_orders, 0)         as total_orders,
        coalesce(h.completed_orders, 0)     as completed_orders,
        coalesce(h.returned_orders, 0)      as returned_orders,
        coalesce(h.lifetime_revenue, 0)     as lifetime_revenue,
        coalesce(h.lifetime_profit, 0)      as lifetime_profit,
        coalesce(h.avg_order_value, 0)      as avg_order_value,
        h.first_order_date,
        h.last_order_date,

        -- RFM
        r.recency_days,
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_total,
        r.rfm_segment,

        -- Derived flags
        case when h.total_orders > 1 then true else false end   as is_repeat_customer,
        case when h.total_orders is null then true else false end as is_never_purchased

    from customers c
    left join order_history h   on c.customer_id = h.customer_id
    left join rfm_segments r    on c.customer_id = r.customer_id
)

select * from final
