with base as (
  select * from USER_DB_FINCH.ANALYTICS.stock_price
),
calc as (
  select
    symbol,
    date,
    open,
    close,
    close - open as daily_change,
    round(((close - open) / open) * 100, 2) as percent_change
  from base
)
select * from calc