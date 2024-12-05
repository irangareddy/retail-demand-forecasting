{{ config(materialized='view') }}

WITH rsi_calc AS (
    SELECT
        symbol,
        date,
        close,
        price_change,
        AVG(price_gain) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as avg_gain_14d,
        AVG(price_loss) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as avg_loss_14d
    FROM {{ ref('int_price_metrics') }}
),

moving_avgs AS (
    SELECT
        symbol,
        date,
        close,
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as ma_20,
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) as ma_50,
        -- Calculate Bollinger Bands
        STDDEV(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as volatility_20d
    FROM {{ ref('stg_stock_prices') }}
)

SELECT
    m.symbol,
    m.date,
    m.close,
    m.ma_20,
    m.ma_50,
    -- RSI Calculation
    CASE 
        WHEN r.avg_loss_14d = 0 THEN 100
        ELSE 100 - (100 / (1 + (r.avg_gain_14d / NULLIF(r.avg_loss_14d, 0))))
    END as rsi_14,
    -- Bollinger Bands
    m.ma_20 + (m.volatility_20d * 2) as upper_band,
    m.ma_20 - (m.volatility_20d * 2) as lower_band
FROM moving_avgs m
LEFT JOIN rsi_calc r
    ON m.symbol = r.symbol 
    AND m.date = r.date