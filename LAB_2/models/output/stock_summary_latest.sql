select *
from {{ ref('stock_summary') }}
qualify row_number() over (partition by symbol order by date desc) = 1