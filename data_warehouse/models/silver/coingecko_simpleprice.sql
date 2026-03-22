{{ config(
    materialized='incremental', 
    unique_key='entity_id',
    meta={
        "elementary": {
        "timestamp_column": "event_occurred_timestamp" 
        }
    }
) }}

WITH 
    BASE_DATA as (
        SELECT 
            event_id,
            entity_id,
            event_occurred_timestamp,
            coin_name,
            ROUND(coin_price::numeric, 2) as coin_price,
            ROUND(market_cap::numeric, 2) as market_cap,
            ROUND(volume::numeric, 2) as volume,
            ROUND(change::numeric, 2) as change,
            TO_TIMESTAMP(last_updated::numeric) as last_updated,
            TO_TIMESTAMP(_ingestion_timestamp, 'YYYY-MM-DDtHH24:MI:SS.FF6') as _ingestion_timestamp,
            _source_name,
            _source_path
        FROM {{ ref('coingecko_simpleprice_bronze') }}
        {% if is_incremental() %}
            WHERE event_occurred_timestamp > (SELECT MAX(event_occurred_timestamp) FROM {{ this }})
        {% endif %}
    ),

    DEDUPE as (
        SELECT 
            r.event_id,
            r.entity_id,
            r.event_occurred_timestamp,
            r.coin_name,
            r.coin_price,
            r.volume,
            r.change,
            r.last_updated,
            r._ingestion_timestamp,
            r._source_name,
            r._source_path
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY event_occurred_timestamp DESC) as seq
            FROM BASE_DATA
        ) r
        WHERE r.seq = 1
    )

SELECT *
FROM DEDUPE