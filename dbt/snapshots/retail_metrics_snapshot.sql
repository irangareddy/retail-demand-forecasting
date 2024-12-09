{% snapshot retail_metrics_snapshot %}
{{
    config(
      target_database='RETAIL_DATA_WAREHOUSE',
      target_schema='snapshots',
      unique_key='state || month',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select 
    *,
    current_timestamp as updated_at
from {{ ref('mart_retail_analysis') }}

{% endsnapshot %}
