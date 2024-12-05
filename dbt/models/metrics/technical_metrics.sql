WITH forecast_accuracy AS (
    SELECT 
        month,
        state,
        -- Calculate MAPE for each category
        ABS(actual_445_sales - forecast_445_sales) / NULLIF(actual_445_sales, 0) as mape_445,
        ABS(actual_448_sales - forecast_448_sales) / NULLIF(actual_448_sales, 0) as mape_448,
        
        -- System performance metrics
        DATEDIFF('minutes', data_ingestion_time, data_available_time) as data_latency,
        DATEDIFF('minutes', pipeline_start_time, pipeline_end_time) as pipeline_runtime,
        
        -- Uptime calculation
        CASE 
            WHEN system_status = 'available' THEN 1
            ELSE 0
        END as is_available
    FROM {{ ref('stg_forecast_performance') }}
),

technical_metrics AS (
    SELECT 
        -- Forecast Accuracy
        AVG(mape_445) * 100 as mape_445_percent,
        AVG(mape_448) * 100 as mape_448_percent,
        
        -- Data Freshness
        AVG(data_latency) as avg_data_latency_minutes,
        MAX(data_latency) as max_data_latency_minutes,
        
        -- Pipeline Performance
        AVG(pipeline_runtime) as avg_pipeline_runtime_minutes,
        MAX(pipeline_runtime) as max_pipeline_runtime_minutes,
        
        -- System Reliability
        COUNT(CASE WHEN is_available = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as system_uptime_percent
    FROM forecast_accuracy
)

SELECT 
    *,
    -- Success criteria evaluation
    CASE 
        WHEN mape_445_percent < 15 AND mape_448_percent < 15 THEN 'Meeting Target'
        ELSE 'Needs Improvement'
    END as forecast_accuracy_status,
    
    CASE 
        WHEN system_uptime_percent > 99.9 THEN 'Meeting Target'
        ELSE 'Needs Improvement'
    END as reliability_status,
    
    CASE 
        WHEN max_data_latency_minutes < 60 THEN 'Meeting Target'
        ELSE 'Needs Improvement'
    END as data_freshness_status,
    
    CASE 
        WHEN max_pipeline_runtime_minutes < 30 THEN 'Meeting Target'
        ELSE 'Needs Improvement'
    END as processing_efficiency_status
FROM technical_metrics