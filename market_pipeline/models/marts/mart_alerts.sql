with price_history as (
    select * from {{ ref('mart_price_history') }}
)

select
    symbol,
    trade_date,
    close_price,
    price_change,
    price_change_pct,
    rolling_avg_5d,
    day_label
from price_history
where is_anomaly = true
order by trade_date desc