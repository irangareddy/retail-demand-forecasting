{% snapshot retail_sales_snapshot %}

{{
    config(
      target_database='RETAIL_DATA_WAREHOUSE',
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='last_updated',
      invalidate_hard_deletes=True
    )
}}

select 
    {{ dbt_utils.generate_surrogate_key(['month', 'state']) }} as id,
    *
from {{ source('raw_data', 'retail_sales') }}

{% endsnapshot %}