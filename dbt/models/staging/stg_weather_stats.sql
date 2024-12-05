WITH source AS (
    SELECT *
    FROM {{ source('raw_data', 'weather_stats') }}
),

validated AS (
    SELECT
        state,
        month,
        mean_temp,
        mean_pressure,
        mean_humidity,
        mean_wind,
        mean_precipitation,
        mean_clouds,
        sunshine_hours_total,
        loaded_at,
        -- Classify temperature ranges
        CASE
            WHEN mean_temp < 0 THEN 'Very Cold'
            WHEN mean_temp < 10 THEN 'Cold'
            WHEN mean_temp < 20 THEN 'Mild'
            WHEN mean_temp < 30 THEN 'Warm'
            ELSE 'Hot'
        END as temp_category,
        -- Classify precipitation
        CASE
            WHEN mean_precipitation = 0 THEN 'None'
            WHEN mean_precipitation < 50 THEN 'Light'
            WHEN mean_precipitation < 100 THEN 'Moderate'
            ELSE 'Heavy'
        END as precipitation_category
    FROM source
)

SELECT *
FROM validated