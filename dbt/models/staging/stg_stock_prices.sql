{{ config(materialized='view') }}

SELECT
    symbol,
    date::DATE as date,
    open::DECIMAL(10,4) as open,
    high::DECIMAL(10,4) as high,
    low::DECIMAL(10,4) as low,
    close::DECIMAL(10,4) as close,
    volume::BIGINT as volume
FROM {{ source('raw', 'alphavantage_stockprice') }}