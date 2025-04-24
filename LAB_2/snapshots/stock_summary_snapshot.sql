{% snapshot stock_summary_snapshot %}
{{
  config(
    target_schema='ANALYTICS',
    unique_key='symbol',
    strategy='check',
    check_cols=['rsi_14', 'ma_7', 'ma_14', 'daily_change', 'percent_change']
  )
}}

select * from {{ ref('stock_summary') }}

{% endsnapshot %}