WITH source AS (
    SELECT * FROM {{ source('raw_data', 'forecast_performance_log') }}
)

SELECT
    month,
    state,
    forecast_445_sales,
    actual_445_sales,
    forecast_448_sales,
    actual_448_sales,
    data_ingestion_time,
    data_available_time,
    pipeline_start_time,
    pipeline_end_time,
    system_status,
    _timestamp as last_updated
FROM source