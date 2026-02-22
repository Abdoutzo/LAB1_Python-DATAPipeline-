{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('stg_playstore_reviews_raw') }}
),
cleaned as (
    select
        md5(cast(reviewId as varchar)) as review_sk,
        cast(reviewId as varchar) as review_id,
        md5(appId) as app_sk,
        appId as app_id,
        userName as reviewer_name,
        userImage as reviewer_image_url,
        trim(content) as review_text,
        length(trim(content)) as review_text_length,
        cast(score as integer) as rating_score,
        cast(thumbsUpCount as integer) as thumbs_up_count,
        cast("at" as timestamp) as review_ts,
        cast("at" as date) as review_date,
        reviewCreatedVersion as review_created_version,
        appVersion as app_version,
        replyContent as reply_content,
        cast(repliedAt as timestamp) as replied_at
    from source
    where appId is not null
      and reviewId is not null
      and score between 1 and 5
)
select *
from cleaned
