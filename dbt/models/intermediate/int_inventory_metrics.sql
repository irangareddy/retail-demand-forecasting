WITH sales_data AS (
    SELECT *
    FROM {{ ref('stg_census_retail_sales') }}
),

inventory_metrics AS (
    SELECT
        month,
        state,
        -- Food & Beverage (445) metrics
        category_445_sales,
        AVG(category_445_sales) OVER(
            PARTITION BY state
            ORDER BY month
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) as avg_monthly_445_sales,
        STDDEV(category_445_sales) OVER(
            PARTITION BY state
            ORDER BY month
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) as std_monthly_445_sales,
        
        -- Clothing (448) metrics
        category_448_sales,
        AVG(category_448_sales) OVER(
            PARTITION BY state
            ORDER BY month
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) as avg_monthly_448_sales,
        STDDEV(category_448_sales) OVER(
            PARTITION BY state
            ORDER BY month
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) as std_monthly_448_sales
    FROM sales_data
)

SELECT
    *,
    -- Safety stock calculations (assuming 95% service level, z=1.96)
    1.96 * std_monthly_445_sales as safety_stock_445,
    1.96 * std_monthly_448_sales as safety_stock_448
FROM inventory_metrics