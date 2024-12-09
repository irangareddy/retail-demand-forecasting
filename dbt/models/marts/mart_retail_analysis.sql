-- mart_retail_analysis.sql
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
    ON SUBSTRING(r.month, 6, 2) = w.month
    AND r.state = w.state
