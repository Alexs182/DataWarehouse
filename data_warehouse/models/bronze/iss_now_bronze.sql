
{{ config(materialized='incremental', unique_key='event_id') }}

WITH
    BASE_DATA as (
        SELECT
            event_id,
            entity_id,
            TO_TIMESTAMP(event_occurred_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF6') as event_occurred_timestamp,
            entity::json->>'timestamp' as timestamp,
            entity::json->>'message' as message,
            entity::json->>'latitude' as latitude,
            entity::json->>'longitude' as longitude,
            _ingestion_timestamp,
            _source_name,
            _source_path
        FROM {{ source('raw', 'iss_now') }}
        {% if is_incremental() %}
            WHERE TO_TIMESTAMP(event_occurred_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF6') > (SELECT MAX(event_occurred_timestamp) from {{ this }})
        {% endif %}
    ),

    DEDUPE as (
        SELECT 
            r.event_id,
            r.entity_id,
            r.event_occurred_timestamp,
            r.timestamp,
            r.message,
            r.latitude,
            r.longitude,
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