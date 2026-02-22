{{ config(materialized='table') }}

with source as (
    select *
    from {{ ref('stg_playstore_reviews') }}
),
normalized as (
    select
        review_id,
        app_id,
        review_ts,
        review_date,
        rating_score,
        thumbs_up_count,
        coalesce(review_text_length, 0) as review_text_length,
        case
            when rating_score <= 2 then 1
            else 0
        end as low_rating_flag,
        case
            when reviewer_name is null or trim(reviewer_name) = '' then 'Unknown Reviewer'
            else trim(reviewer_name)
        end as reviewer_name,
        md5(lower(
            case
                when reviewer_name is null or trim(reviewer_name) = '' then 'Unknown Reviewer'
                else trim(reviewer_name)
            end
        )) as reviewer_sk
    from source
),
joined as (
    select
        n.review_id,
        a.app_sk,
        d.date_sk,
        n.reviewer_sk,
        n.rating_score,
        n.thumbs_up_count,
        n.review_text_length,
        n.low_rating_flag,
        1 as review_count,
        n.review_ts
    from normalized n
    inner join {{ ref('dim_apps') }} a
        on n.app_id = a.app_id
    inner join {{ ref('dim_dates') }} d
        on n.review_date = d.date_day
    inner join {{ ref('dim_reviewers') }} r
        on n.reviewer_sk = r.reviewer_sk
)
select *
from joined
