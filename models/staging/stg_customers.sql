-- models/staging/stg_customers.sql
-- Staging layer: normalise customer source data.

with source as (
    select * from {{ ref('raw_customers') }}
),

cleaned as (
    select
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name                  as full_name,
        lower(email)                                    as email,
        country,
        region,
        acquisition_channel,
        cast(registration_date as date)                 as registration_date,
        cast(is_loyalty_member as boolean)              as is_loyalty_member,
        age_group,

        -- Cohort month for retention analysis
        date_trunc('month', cast(registration_date as date)) as cohort_month,
        date_part('year', cast(registration_date as date))   as registration_year

    from source
    where customer_id is not null
      and email is not null
)

select * from cleaned
