{{ config(
    materialized='table',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['region']
) }}

select
  row_number() over () as policy_id,
  age, sex, bmi, children, smoker, region, charges
from {{ ref('stg_insurance_claims') }}
