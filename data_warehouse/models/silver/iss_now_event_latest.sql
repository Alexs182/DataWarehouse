{{ 
    config(
        materialized='incremental', 
        unique_key='event_id'
    ) 
}}

WITH 
    BASE_DATA as (
        SELECT
            event_id,
            entity_id,
            event_occurred_timestamp,
            TO_TIMESTAMP(timestamp::bigint) as timestamp,
            message::text as message,
            latitude::numeric(9,6) as latitude,
            longitude::numeric(9,6) as longitude,
            TO_TIMESTAMP(_ingestion_timestamp, 'YYYY-MM-DDtHH24:MI:SS.FF6') as _ingestion_timestamp,
            _source_name,
            _source_path
        FROM {{ ref('iss_now_bronze') }}
        {% if is_incremental() %}
            WHERE event_occurred_timestamp > (SELECT MAX(event_occurred_timestamp) from {{ this }})
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