-- MERGE INTO — an upsert without a full table rewrite.

CREATE TABLE curated.claims_updates
WITH (table_type = 'ICEBERG', location = 's3://arun-lakehouse-curated/claims_updates/') AS
SELECT policy_id, 'closed' AS policy_status
FROM curated.insurance_claims_iceberg LIMIT 3;

MERGE INTO curated.insurance_claims_iceberg t
USING curated.claims_updates s ON t.policy_id = s.policy_id
WHEN MATCHED THEN UPDATE SET policy_status = s.policy_status
WHEN NOT MATCHED THEN INSERT (policy_id, policy_status)
  VALUES (s.policy_id, s.policy_status);
