{{ config(materialized='table') }}

with source as (
    select *
    from {{ ref('stg_playstore_apps') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by app_id
            order by updated_ts_utc desc nulls last, app_name asc
        ) as row_num
    from source
)
select
    app_sk,
    app_id,
    app_name,
    app_summary,
    app_description,
    developer_name,
    developer_id,
    developer_email,
    developer_website,
    genre_name,
    genre_id,
    primary_category_name,
    primary_category_id,
    store_score,
    ratings_count,
    reviews_count,
    installs_label,
    min_installs,
    real_installs,
    price_amount,
    is_free,
    offers_iap,
    currency,
    released_date,
    last_updated_on_date,
    updated_ts_utc,
    app_url
from ranked
where row_num = 1
