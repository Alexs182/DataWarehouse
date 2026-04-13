{{ config(
    materialized='table'
) }}

CREATE TABLE gold.daily_coin_change_week as
WITH
    base_data as (
        SELECT
            coin_name,
            DATE(last_updated) as report_date,
            ROUND(AVG(coin_price), 2) average_coin_price,
            ROUND(MIN(coin_price), 2) min_coin_price,
            ROUND(MAX(coin_price), 2) max_coin_price,
            ROUND(AVG(market_cap), 2) average_market_cap,
            ROUND(MIN(market_cap), 2) min_market_cap,
            ROUND(MAX(market_cap), 2) max_market_cap,
            ROUND(AVG(volume), 2) average_volume,
            ROUND(MIN(volume), 2) min_volume,
            ROUND(MAX(volume), 2) max_volume
        -- FROM silver.coingecko_simpleprice_event_latest
        FROM {{ ref('coingecko_simpleprice_event_latest')}}
        WHERE DATE(last_updated) >= CURRENT_DATE - 7
        GROUP BY 1,2

    )

SELECT *
FROM base_data