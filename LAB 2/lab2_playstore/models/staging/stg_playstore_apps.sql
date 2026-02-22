{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('stg_playstore_apps_raw') }}
),
cleaned as (
    select
        md5(appId) as app_sk,
        appId as app_id,
        title as app_name,
        summary as app_summary,
        description as app_description,
        developer as developer_name,
        developerId as developer_id,
        developerEmail as developer_email,
        developerWebsite as developer_website,
        genre as genre_name,
        genreId as genre_id,
        categories[1].name as primary_category_name,
        categories[1].id as primary_category_id,
        cast(score as double) as store_score,
        cast(ratings as bigint) as ratings_count,
        cast(reviews as bigint) as reviews_count,
        installs as installs_label,
        cast(minInstalls as bigint) as min_installs,
        cast(realInstalls as bigint) as real_installs,
        cast(price as bigint) as price_amount,
        cast(free as boolean) as is_free,
        cast(offersIAP as boolean) as offers_iap,
        currency,
        cast(try_strptime(released, '%b %d, %Y') as date) as released_date,
        cast(try_strptime(lastUpdatedOn, '%b %d, %Y') as date) as last_updated_on_date,
        cast(to_timestamp(updated) as timestamp) as updated_ts_utc,
        url as app_url
    from source
    where appId is not null
)
select *
from cleaned
