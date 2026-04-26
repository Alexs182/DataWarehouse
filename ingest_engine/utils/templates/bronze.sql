{{ '{{' }}
	config(
		materialized='incremental',
		unique_key='event_id',
		meta={
			"elementary": {
				"timestamp_column": "event_occurred_timestamp"
			}
		}
	)
{{ '}}' }}

WITH
	BASE_DATA as (
		SELECT
			event_id,
			entity_id,
			TO_TIMESTAMP(event_occurred_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF6') as event_occurred_timestamp,
			{% for field in params["fields"] %}entity::json->>'{{ field }}' as {{ field }},
			{% endfor %}
			_ingestion_timestamp,
			_source_name,
			_source_path
		FROM {{ '{{' }} source('raw', '{{ params["table_name"] }}')  {{ '}}' }}
	        {{ 
		"{% if is_incremental() %} 
        	    WHERE TO_TIMESTAMP(event_occurred_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF6') > (SELECT MAX(event_occurred_timestamp) from {{ this }})
        	{% endif %}"
		}}
    	),

    	DEDUPE as (
        	SELECT 
            		r.event_id,
            		r.entity_id,
            		r.event_occurred_timestamp,
            		{% for field in params["fields"] %}r.{{ field }},
			{% endfor %}
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
