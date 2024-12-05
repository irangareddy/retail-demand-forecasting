WITH source AS (
    SELECT *
    FROM {{ source('raw_data', 'retail_sales') }}
),

validated AS (
    SELECT
        month,
        state,
        category_445_sales,
        category_445_share,
        category_448_sales,
        category_448_share,
        national_445_total,
        national_448_total,
        last_updated,
        loaded_at,
        -- Data validation
        CASE
            WHEN category_445_sales < 0 OR category_448_sales < 0 THEN 'Invalid negative sales'
            WHEN category_445_share > 1 OR category_448_share > 1 THEN 'Invalid share > 1'
            ELSE 'Valid'
        END as data_quality_check
    FROM source
)

SELECT
    month,
    state,
    category_445_sales,
    category_445_share,
    category_448_sales,
    category_448_share,
    national_445_total,
    national_448_total,
    last_updated,
    loaded_at,
    data_quality_check
FROM validated