{{ config(
    materialized='table'
) }}

WITH 
    base_data as (
        SELECT 
            coin_name,
            coin_price,
            market_cap,
            volume,
            change,
            last_updated
        FROM {{ ref('coingecko_simpleprice_entity_latest')}}
    )

SELECT *
FROM base_data