
-- mart_inventory_planning.sql
WITH inventory_base AS (
    SELECT 
        r.*,
        e.consumer_confidence,
        e.consumer_sentiment_trend,
        w.temp_category,
        w.precipitation_category
    FROM {{ ref('int_inventory_metrics') }} r
    LEFT JOIN {{ ref('int_economic_trends') }} e
        ON r.month = e.month
    LEFT JOIN {{ ref('stg_weather_stats') }} w
        ON SUBSTRING(r.month, 6, 2) = w.month
        AND r.state = w.state
)
SELECT 
    *,
    CASE 
        WHEN consumer_sentiment_trend = 'Improving' THEN safety_stock_445 * 1.1
        WHEN consumer_sentiment_trend = 'Declining' THEN safety_stock_445 * 0.9
        ELSE safety_stock_445 
    END as adjusted_safety_stock_445,
    CASE 
        WHEN consumer_sentiment_trend = 'Improving' THEN safety_stock_448 * 1.1
        WHEN consumer_sentiment_trend = 'Declining' THEN safety_stock_448 * 0.9
        ELSE safety_stock_448 
    END as adjusted_safety_stock_448,
    CASE 
        WHEN category_445_sales > avg_monthly_445_sales + safety_stock_445 THEN 'Increase'
        WHEN category_445_sales < avg_monthly_445_sales - safety_stock_445 THEN 'Decrease'
        ELSE 'Maintain'
    END as stock_recommendation_445,
    CASE 
        WHEN category_448_sales > avg_monthly_448_sales + safety_stock_448 THEN 'Increase'
        WHEN category_448_sales < avg_monthly_448_sales - safety_stock_448 THEN 'Decrease'
        ELSE 'Maintain'
    END as stock_recommendation_448
FROM inventory_base