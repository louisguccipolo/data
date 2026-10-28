select count(*) from {{ ref('dim_date') }} having count(*) filter (where date_key = -1) <> 1
