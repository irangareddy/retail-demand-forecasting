WITH retail_base AS (
    SELECT *
    FROM {{ ref('stg_census_retail_sales') }}
    WHERE data_quality_check = 'Valid'
),

sales_metrics AS (
    SELECT
        month,
        state,
        -- Category 445 metrics
        category_445_sales,
        category_445_share,
        LAG(category_445_sales) OVER(
            PARTITION BY state
            ORDER BY month
        ) as prev_month_445_sales,
        
        -- Category 448 metrics
        category_448_sales,
        category_448_share,
        LAG(category_448_sales) OVER(
            PARTITION BY state
            ORDER BY month
        ) as prev_month_448_sales,
        
        -- Growth calculations
        (category_445_sales - LAG(category_445_sales) OVER(
            PARTITION BY state ORDER BY month
        )) / NULLIF(LAG(category_445_sales) OVER(
            PARTITION BY state ORDER BY month
        ), 0) as mom_growth_445,
        
        (category_448_sales - LAG(category_448_sales) OVER(
            PARTITION BY state ORDER BY month
        )) / NULLIF(LAG(category_448_sales) OVER(
            PARTITION BY state ORDER BY month
        ), 0) as mom_growth_448,
        
        -- Moving averages
        AVG(category_445_sales) OVER(
            PARTITION BY state
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as moving_avg_445_3m,
        
        AVG(category_448_sales) OVER(
            PARTITION BY state
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as moving_avg_448_3m
    FROM retail_base
)

SELECT
    *,
    -- Seasonality indices
    category_445_sales / NULLIF(moving_avg_445_3m, 0) as seasonal_index_445,
    category_448_sales / NULLIF(moving_avg_448_3m, 0) as seasonal_index_448
FROM sales_metrics