{{ config(materialized='table') }}

with source as (
    select
        case
            when reviewer_name is null or trim(reviewer_name) = '' then 'Unknown Reviewer'
            else trim(reviewer_name)
        end as reviewer_name_clean
    from {{ ref('stg_playstore_reviews') }}
),
normalized as (
    select
        lower(reviewer_name_clean) as reviewer_name_norm,
        reviewer_name_clean
    from source
),
distinct_reviewers as (
    select
        reviewer_name_norm,
        min(reviewer_name_clean) as reviewer_name
    from normalized
    group by reviewer_name_norm
)
select
    md5(reviewer_name_norm) as reviewer_sk,
    reviewer_name
from distinct_reviewers
