{% snapshot snapshot_stock_prices %}

{{
    config(
      target_schema='snapshot',
      unique_key='id',
      strategy='timestamp',
      updated_at='date',
      invalidate_hard_deletes=True
    )
}}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['symbol', 'date']) }} as id,  -- Create a unique identifier
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume
    FROM {{ source('raw', 'alphavantage_stockprice') }}
)

SELECT * FROM source_data

{% endsnapshot %}