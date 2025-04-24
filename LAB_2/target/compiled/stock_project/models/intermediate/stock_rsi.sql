with base as (
  select * from USER_DB_FINCH.ANALYTICS.stock_price
),
diffs as (
  select symbol, date, close, close - lag(close) over (partition by symbol order by date) as diff from base
),
calc as (
  select symbol, date,
    case when diff > 0 then diff else 0 end as gain,
    case when diff < 0 then abs(diff) else 0 end as loss
  from diffs
),
final as (
  select symbol, date,
    avg(gain) over (partition by symbol order by date rows between 13 preceding and current row) as avg_gain,
    avg(loss) over (partition by symbol order by date rows between 13 preceding and current row) as avg_loss,
    case when avg_loss = 0 then 100 else round(100 - (100 / (1 + avg_gain / avg_loss)), 2) end as rsi_14
  from calc
)
select * from final