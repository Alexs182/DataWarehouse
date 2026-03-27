{{ config(
    materialized='incremental', 
    unique_key='event_id',
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
            TO_TIMESTAMP(event_occurred_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF6') as event_occurred_timestamp,
            entity::json->>'coin_name' as coin_name,
            entity::json->>'price' as coin_price,
            entity::json->>'market_cap' as market_cap,
            entity::json->>'volume' as volume,
            entity::json->>'change' as change,
            entity::json->>'last_updated' as last_updated,
            _ingestion_timestamp,
            _source_name,
            _source_path
        FROM {{ source('raw', 'coingecko_simpleprice') }}
        {% if is_incremental() %}
            WHERE TO_TIMESTAMP(event_occurred_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF6') > (SELECT MAX(event_occurred_timestamp) FROM {{ this }})
        {% endif %}
    ),

    DEDUPE as (
        SELECT
            r.event_id,
            r.entity_id,
            r.event_occurred_timestamp,
            r.coin_name,
            r.coin_price,
            r.market_cap,
            r.volume,
            r.change,
            r.last_updated,
            r._ingestion_timestamp,
            r._source_name,
            r._source_path
        FROM (
            SELECT  
                *,
                ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_occurred_timestamp DESC) as seq
            FROM BASE_DATA
        ) r
        WHERE r.seq = 1
    )

SELECT *
FROM DEDUPE