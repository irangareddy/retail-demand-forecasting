{{ config(materialized='view') }}

WITH price_data AS (
    SELECT
        symbol,
        date,
        close,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
        LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) as close_5d_ago,
        -- For RSI calculation
        LAG(close, 14) OVER (PARTITION BY symbol ORDER BY date) as close_14d_ago
    FROM {{ ref('stg_stock_prices') }}
),

price_changes AS (
    SELECT
        symbol,
        date,
        close,
        prev_close,
        close_5d_ago,
        (close - prev_close) as price_change,
        ((close - prev_close) / prev_close) * 100 as daily_return,
        ((close - close_5d_ago) / close_5d_ago) * 100 as weekly_return,
        CASE 
            WHEN close > prev_close THEN close - prev_close 
            ELSE 0 
        END as price_gain,
        CASE 
            WHEN close < prev_close THEN ABS(close - prev_close) 
            ELSE 0 
        END as price_loss
    FROM price_data
    WHERE prev_close IS NOT NULL
)

SELECT *
FROM price_changes