with base as (
  select * from {{ ref('stock_price') }}
),
ma as (
  select
    symbol,
    date,
    close,
    avg(close) over (partition by symbol order by date rows between 6 preceding and current row) as ma_7,
    avg(close) over (partition by symbol order by date rows between 13 preceding and current row) as ma_14
  from base
)
select * from ma