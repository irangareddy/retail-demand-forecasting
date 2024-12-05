{% snapshot snapshot_stock_analytics %}

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
        {{ dbt_utils.generate_surrogate_key(['symbol', 'date']) }} as id,
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        daily_return,
        weekly_return,
        ma_20,
        ma_50,
        rsi_14,
        upper_band,
        lower_band,
        trend_signal,
        bollinger_signal,
        rsi_signal
    FROM {{ ref('stock_analytics') }}
)

SELECT * FROM source_data

{% endsnapshot %}

