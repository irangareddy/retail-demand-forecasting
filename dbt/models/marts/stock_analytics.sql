{{ config(materialized='table') }}

SELECT
    p.symbol,
    p.date,
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume,
    p.daily_return,
    p.weekly_return,
    t.ma_20,
    t.ma_50,
    t.rsi_14,
    t.upper_band,
    t.lower_band,
    CASE 
        WHEN t.ma_20 > t.ma_50 THEN 'BULLISH'
        ELSE 'BEARISH'
    END as trend_signal,
    CASE 
        WHEN s.close > t.upper_band THEN 'OVERBOUGHT'
        WHEN s.close < t.lower_band THEN 'OVERSOLD'
        ELSE 'NEUTRAL'
    END as bollinger_signal,
    CASE 
        WHEN t.rsi_14 > 70 THEN 'OVERBOUGHT'
        WHEN t.rsi_14 < 30 THEN 'OVERSOLD'
        ELSE 'NEUTRAL'
    END as rsi_signal
FROM {{ ref('int_price_metrics') }} p
JOIN {{ ref('stg_stock_prices') }} s
    ON p.symbol = s.symbol 
    AND p.date = s.date
JOIN {{ ref('int_technical_indicators') }} t
    ON p.symbol = t.symbol 
    AND p.date = t.date