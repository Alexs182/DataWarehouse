{{ config(
    materialized='table'
) }}


SELECT * 
FROM (
    SELECT 
        coin_name,
        coin_price,
        market_cap,
        volume,
        last_updated,
        ROW_NUMBER() OVER (PARTITION BY coin_name ORDER BY last_updated) as row_num
    FROM {{ ref('coingecko_simpleprice_entity_latest')}}
)
WHERE row_num = 1