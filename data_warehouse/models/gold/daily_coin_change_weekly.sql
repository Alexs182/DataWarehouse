{{ config(
    materialized='table'
) }}

WITH
    base_data as (
        SELECT
            coin_name,
            DATE(last_updated) as report_date,
            ROUND(AVG(coin_price), 2) as average_coin_price,
            ROUND(MIN(coin_price), 2) as min_coin_price,
            ROUND(MAX(coin_price), 2) as max_coin_price,
            ROUND(AVG(market_cap), 2) as average_market_cap,
            ROUND(MIN(market_cap), 2) as min_market_cap,
            ROUND(MAX(market_cap), 2) as max_market_cap,
            ROUND(AVG(volume), 2) as average_volume,
            ROUND(MIN(volume), 2) as min_volume,
            ROUND(MAX(volume), 2) as max_volume,
            ROUND(AVG(change), 2) as average_change,
            ROUND(MIN(change), 2) as min_change,
            ROUND(MAX(change), 2) as max_change
         -- FROM silver.coingecko_simpleprice_event_latest
        FROM {{ ref('coingecko_simpleprice_event_latest')}}
        WHERE DATE(last_updated) >= CURRENT_DATE - 7
        GROUP BY 1,2

    )

SELECT *
FROM base_data