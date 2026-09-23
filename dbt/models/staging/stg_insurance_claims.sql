select
  age, sex, bmi, children, smoker, region, charges
from {{ source('raw', 'insurance_claims_raw') }}
