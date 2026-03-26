select *
from {{ ref('vw_price_with_bubbles_daily') }}
where close is null
limit 1