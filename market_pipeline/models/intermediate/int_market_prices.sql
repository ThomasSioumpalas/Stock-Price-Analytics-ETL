with staging as (
    select * from {{ ref('stg_market_prices') }}
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

        -- price change from previous day
        round(
            close_price - LAG(close_price) over (
                partition by symbol order by trade_date
            )
        , 2) as price_change,

        -- percentage change from previous day
        round(
            (close_price - LAG(close_price) over (partition by symbol order by trade_date))
            / LAG(close_price) over (partition by symbol order by trade_date) * 100
        , 2) as price_change_pct,

        -- 5 day rolling average
        round(AVG(close_price) over (
            partition by symbol
            order by trade_date
            rows between 4 preceding and current row
        ), 2) as rolling_avg_5d,

        -- anomaly flag: price moved more than 2% in a day
        case
            when abs(
                (close_price - LAG(close_price) over (partition by symbol order by trade_date))
                / LAG(close_price) over (partition by symbol order by trade_date) * 100
            ) > 2 then true
            else false
        end as is_anomaly

    from staging
)

select * from enriched