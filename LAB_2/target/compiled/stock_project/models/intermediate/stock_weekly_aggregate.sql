with base as (
  select * from USER_DB_FINCH.ANALYTICS.stock_price
)
select
  symbol,
  date_trunc('week', date) as week,
  avg(close) as avg_week_close,
  sum(volume) as total_week_volume
from base
group by symbol, date_trunc('week', date)