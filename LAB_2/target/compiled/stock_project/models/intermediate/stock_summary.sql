with rsi as (
  select symbol as rsi_symbol, date as rsi_date, rsi_14
  from USER_DB_FINCH.ANALYTICS.stock_rsi
),
ma as (
  select symbol as ma_symbol, date as ma_date, ma_7, ma_14
  from USER_DB_FINCH.ANALYTICS.stock_moving_averages
),
chg as (
  select symbol as chg_symbol, date as chg_date, daily_change, percent_change
  from USER_DB_FINCH.ANALYTICS.stock_daily_changes
),
top_vol as (
  select symbol as vol_symbol, date as vol_date, volume, vol_rank
  from USER_DB_FINCH.ANALYTICS.stock_top_volume_days
),
weekly as (
  select symbol as wk_symbol, week, avg_week_close, total_week_volume
  from USER_DB_FINCH.ANALYTICS.stock_weekly_aggregate
)

select
  rsi.rsi_symbol as symbol,
  rsi.rsi_date as date,
  rsi.rsi_14,
  ma.ma_7,
  ma.ma_14,
  chg.daily_change,
  chg.percent_change,
  top_vol.volume,
  top_vol.vol_rank,
  weekly.avg_week_close,
  weekly.total_week_volume
from rsi
join ma on rsi.rsi_symbol = ma.ma_symbol and rsi.rsi_date = ma.ma_date
join chg on rsi.rsi_symbol = chg.chg_symbol and rsi.rsi_date = chg.chg_date
left join top_vol on rsi.rsi_symbol = top_vol.vol_symbol and rsi.rsi_date = top_vol.vol_date
left join weekly on rsi.rsi_symbol = weekly.wk_symbol and date_trunc('week', rsi.rsi_date) = weekly.week