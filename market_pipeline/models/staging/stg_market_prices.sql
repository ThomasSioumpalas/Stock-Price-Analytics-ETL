with source as (
    select * from {{ source('market_data', 'raw_market_prices') }}
),

renamed as (
    select
        symbol,
        cast(timestamp as DATE)    as trade_date,
        cast(open as DOUBLE)       as open_price,
        cast(high as DOUBLE)       as high_price,
        cast(low as DOUBLE)        as low_price,
        cast(close as DOUBLE)      as close_price,
        cast(volume as BIGINT)     as volume_shares,
        ingested_at
    from source
    -- deduplicated rows with QUALIFY
    qualify row_number() over (
        partition by symbol, cast(timestamp as DATE)
        order by ingested_at desc
    ) = 1
)

select * from renamed








