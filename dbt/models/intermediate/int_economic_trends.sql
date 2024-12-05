WITH economic_base AS (
    SELECT *
    FROM {{ ref('stg_fred_economic') }}
),

trend_metrics AS (
    SELECT
        month,
        consumer_confidence,
        unemployment_rate,
        inflation_rate,
        -- Calculate rolling averages
        AVG(consumer_confidence) OVER(
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as confidence_3m_avg,
        AVG(unemployment_rate) OVER(
            ORDER BY month
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as unemployment_3m_avg,
        -- Trend indicators
        CASE
            WHEN consumer_confidence > LAG(consumer_confidence, 3) OVER(ORDER BY month) THEN 'Improving'
            WHEN consumer_confidence < LAG(consumer_confidence, 3) OVER(ORDER BY month) THEN 'Declining'
            ELSE 'Stable'
        END as consumer_sentiment_trend
    FROM economic_base
)

SELECT * FROM trend_metrics