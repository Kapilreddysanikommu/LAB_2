with daily as (
  select * from {{ ref('stock_daily_changes') }}
)
select *
from daily
where percent_change > 2 or percent_change < -2
