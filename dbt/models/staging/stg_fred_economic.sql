WITH source AS (
    SELECT *
    FROM {{ source('raw_data', 'fred_economic_data') }}
),

validated AS (
    SELECT
        month,
        consumer_confidence,
        unemployment_rate,
        inflation_rate,
        gdp_growth_rate,
        federal_funds_rate,
        retail_sales,
        last_updated,
        loaded_at,
        -- Handle missing values with previous period's data
        COALESCE(consumer_confidence, 
            LAG(consumer_confidence) OVER(ORDER BY month)) as clean_consumer_confidence,
        COALESCE(unemployment_rate,
            LAG(unemployment_rate) OVER(ORDER BY month)) as clean_unemployment_rate
    FROM source
)

SELECT
    month,
    clean_consumer_confidence as consumer_confidence,
    clean_unemployment_rate as unemployment_rate,
    inflation_rate,
    gdp_growth_rate,
    federal_funds_rate,
    retail_sales,
    last_updated,
    loaded_at
FROM validated