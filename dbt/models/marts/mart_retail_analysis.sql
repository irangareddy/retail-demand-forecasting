-- models/marts/mart_retail_analysis.sql
WITH combined_data AS (
    SELECT
        r.month,
        r.state,
        r.category_445_sales,
        r.category_448_sales,
        r.mom_growth_445,
        r.mom_growth_448,
        r.seasonal_index_445,
        r.seasonal_index_448,
        e.consumer_confidence,
        e.unemployment_rate,
        e.inflation_rate,
        w.mean_temp,
        w.mean_precipitation,
        w.temp_category,
        w.precipitation_category
    FROM {{ ref('int_retail_metrics') }} r
    LEFT JOIN {{ ref('stg_fred_economic') }} e
        ON r.month = e.month
    LEFT JOIN {{ ref('stg_weather_stats') }} w
        ON r.month = w.month
        AND r.state = w.state
)

SELECT
    *,
    -- Safe date extraction
    EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) as month_num,
    CASE 
        WHEN EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) IN (12,1,2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) IN (6,7,8) THEN 'Summer'
        ELSE 'Fall'
    END as season,
    CASE
        WHEN EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) IN (11,12) THEN 1
        ELSE 0
    END as is_holiday_season
FROM combined_data