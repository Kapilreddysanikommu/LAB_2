
    
    

select
    symbol as unique_field,
    count(*) as n_records

from USER_DB_FINCH.ANALYTICS.stock_summary_latest
where symbol is not null
group by symbol
having count(*) > 1


