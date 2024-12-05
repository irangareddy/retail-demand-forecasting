{% snapshot weather_stats_snapshot %}

{{
    config(
      target_database='RETAIL_DATA_WAREHOUSE',
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols=['mean_temp', 'mean_pressure', 'mean_humidity', 'mean_wind', 
                 'mean_precipitation', 'mean_clouds', 'sunshine_hours_total'],
      invalidate_hard_deletes=True
    )
}}

select 
    {{ dbt_utils.generate_surrogate_key(['state', 'month']) }} as id,
    *
from {{ source('raw_data', 'weather_stats') }}

{% endsnapshot %}
