-- models/staging/stg_business_performance.sql
WITH source AS (
    SELECT * FROM {{ source('raw_data', 'business_performance') }}
),

validated AS (
    SELECT
        month,
        state,
        category_code,
        inventory_value,
        storage_cost,
        insurance_cost,
        obsolescence_cost,
        total_carrying_cost,
        stockout_events,
        total_demand,
        lost_sales_value,
        labor_hours_planned,
        labor_hours_actual,
        decision_request_time,
        decision_made_time,
        loaded_at,
        
        -- Data validation flags
        CASE
            WHEN inventory_value < 0 THEN 'Invalid inventory value'
            WHEN total_carrying_cost < 0 THEN 'Invalid carrying cost'
            WHEN stockout_events < 0 THEN 'Invalid stockout count'
            WHEN labor_hours_actual < 0 THEN 'Invalid labor hours'
            ELSE 'Valid'
        END as data_quality_check
    FROM source
)

SELECT *
FROM validated
WHERE data_quality_check = 'Valid'