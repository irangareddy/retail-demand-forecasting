{% snapshot retail_metrics_mart_snapshot %}

{{
    config(
      target_database='RETAIL_DATA_WAREHOUSE',
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='loaded_at',
      invalidate_hard_deletes=True
    )
}}

select 
    {{ dbt_utils.generate_surrogate_key(['month', 'state']) }} as id,
    *,
    current_timestamp() as loaded_at
from {{ ref('mart_retail_analysis') }}

{% endsnapshot %}