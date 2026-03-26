select *
from {{ ref('vw_price_with_bubbles_daily') }}
where trade_date > current_date
limit 1