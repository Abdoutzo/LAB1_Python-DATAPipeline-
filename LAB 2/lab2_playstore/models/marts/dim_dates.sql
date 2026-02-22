{{ config(materialized='table') }}

with source as (
    select distinct review_date as date_day
    from {{ ref('stg_playstore_reviews') }}
    where review_date is not null
)
select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_sk,
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    cast(strftime(date_day, '%V') as integer) as iso_week_of_year,
    extract(day from date_day) as day_of_month,
    cast(strftime(date_day, '%w') as integer) as day_of_week_sun0,
    case
        when cast(strftime(date_day, '%w') as integer) in (0, 6) then true
        else false
    end as is_weekend
from source
