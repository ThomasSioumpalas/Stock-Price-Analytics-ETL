with intermediate as (
    select * from {{ ref('int_market_prices') }}
),

enriched as (
    select
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume_shares,
        price_change,
        price_change_pct,
        rolling_avg_5d,
        is_anomaly,

        case
            when price_change_pct > 2   then 'Strong Up'
            when price_change_pct > 0   then 'Up'
            when price_change_pct < -2  then 'Strong Down'
            when price_change_pct < 0   then 'Down'
            else                             'Flat'
        end as day_label

    from intermediate
    order by trade_date desc
)

select * from enriched