select *
from {{ ref('stg_playstore_apps') }}
where store_score is not null
  and (store_score < 0 or store_score > 5)
