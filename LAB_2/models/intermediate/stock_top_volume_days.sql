with ranked as (
  select *,
         row_number() over (partition by symbol order by volume desc) as vol_rank
  from {{ ref('stock_price') }}
)
select *
from ranked
where vol_rank <= 5