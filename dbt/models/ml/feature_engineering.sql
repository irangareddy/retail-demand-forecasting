WITH sales_features AS (
    SELECT 
        s.month as month_key,
        s.state,
        s.category_445_sales,
        s.category_448_sales,
        -- Lag features
        LAG(s.category_445_sales, 1) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD')) AS prev_month_445,
        LAG(s.category_445_sales, 2) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD')) AS prev_2month_445,
        LAG(s.category_445_sales, 12) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD')) AS prev_year_445,
        LAG(s.category_448_sales, 1) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD')) AS prev_month_448,
        LAG(s.category_448_sales, 2) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD')) AS prev_2month_448,
        LAG(s.category_448_sales, 12) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD')) AS prev_year_448,
        -- Moving averages
        AVG(s.category_445_sales) OVER(
            PARTITION BY s.state 
            ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD') 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS moving_avg_3m_445,
        AVG(s.category_448_sales) OVER(
            PARTITION BY s.state 
            ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD') 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS moving_avg_3m_448,
        -- Growth rates
        (s.category_445_sales - LAG(s.category_445_sales, 1) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD'))) / 
            NULLIF(LAG(s.category_445_sales, 1) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD')), 0) AS mom_growth_445,
        (s.category_448_sales - LAG(s.category_448_sales, 1) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD'))) / 
            NULLIF(LAG(s.category_448_sales, 1) OVER(PARTITION BY s.state ORDER BY TO_DATE(s.month || '-01', 'YYYY-MM-DD')), 0) AS mom_growth_448
    FROM {{ ref('stg_census_retail_sales') }} s
),

economic_features AS (
    SELECT
        month as month_key,
        consumer_confidence,
        unemployment_rate,
        inflation_rate,
        (consumer_confidence - AVG(consumer_confidence) OVER()) / NULLIF(STDDEV(consumer_confidence) OVER(), 0) as normalized_confidence,
        (unemployment_rate - AVG(unemployment_rate) OVER()) / NULLIF(STDDEV(unemployment_rate) OVER(), 0) as normalized_unemployment
    FROM {{ ref('stg_fred_economic') }}
),

weather_features AS (
    SELECT
        month as month_key,
        state,
        mean_temp,
        mean_precipitation,
        CASE WHEN mean_temp < 0 THEN 1 ELSE 0 END as is_freezing,
        CASE WHEN mean_temp BETWEEN 0 AND 15 THEN 1 ELSE 0 END as is_cold,
        CASE WHEN mean_temp BETWEEN 15 AND 25 THEN 1 ELSE 0 END as is_mild,
        CASE WHEN mean_temp > 25 THEN 1 ELSE 0 END as is_hot,
        CASE WHEN mean_precipitation = 0 THEN 1 ELSE 0 END as is_dry,
        CASE WHEN mean_precipitation > 50 THEN 1 ELSE 0 END as is_heavy_rain
    FROM {{ ref('stg_weather_stats') }}
),

temporal_features AS (
    SELECT
        month as month_key,
        EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) as month_number,
        EXTRACT(YEAR FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) as year,
        CASE 
            WHEN EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) IN (12,1,2) THEN 1 
            ELSE 0 
        END as is_winter,
        CASE 
            WHEN EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) IN (6,7,8) THEN 1 
            ELSE 0 
        END as is_summer,
        CASE 
            WHEN EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) IN (11,12) THEN 1 
            ELSE 0 
        END as is_holiday_season,
        SIN(2 * 3.14159 * EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) / 12) as month_sin,
        COS(2 * 3.14159 * EXTRACT(MONTH FROM TO_DATE(month || '-01', 'YYYY-MM-DD')) / 12) as month_cos
    FROM (SELECT DISTINCT month FROM {{ ref('stg_census_retail_sales') }})
)

SELECT 
    s.month_key as month,
    s.state,
    s.category_445_sales as target_445,
    s.category_448_sales as target_448,
    s.prev_month_445,
    s.prev_2month_445,
    s.prev_year_445,
    s.moving_avg_3m_445,
    s.mom_growth_445,
    s.prev_month_448,
    s.prev_2month_448,
    s.prev_year_448,
    s.moving_avg_3m_448,
    s.mom_growth_448,
    e.normalized_confidence,
    e.normalized_unemployment,
    e.inflation_rate,
    w.is_freezing,
    w.is_cold,
    w.is_mild,
    w.is_hot,
    w.is_dry,
    w.is_heavy_rain,
    t.month_number,
    t.year,
    t.is_winter,
    t.is_summer,
    t.is_holiday_season,
    t.month_sin,
    t.month_cos,
    t.is_holiday_season * e.normalized_confidence as holiday_confidence_interaction,
    w.is_hot * s.mom_growth_448 as hot_weather_clothing_growth,
    t.is_winter * s.mom_growth_445 as winter_food_growth
FROM sales_features s
LEFT JOIN economic_features e ON s.month_key = e.month_key
LEFT JOIN weather_features w ON s.month_key = w.month_key AND s.state = w.state
LEFT JOIN temporal_features t ON s.month_key = t.month_key