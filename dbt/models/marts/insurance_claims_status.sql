{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='policy_id',
    format='parquet'
) }}

select
  c.policy_id,
  coalesce(s.policy_status, 'open') as policy_status
from {{ ref('insurance_claims_dbt') }} c
left join {{ ref('policy_closures') }} s
  on c.policy_id = s.policy_id
