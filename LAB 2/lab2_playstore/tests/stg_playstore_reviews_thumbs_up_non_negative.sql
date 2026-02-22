select *
from {{ ref('stg_playstore_reviews') }}
where thumbs_up_count < 0
