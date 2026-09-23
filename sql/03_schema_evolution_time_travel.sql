-- Schema evolution and time travel, no rewrite required.

ALTER TABLE curated.insurance_claims_iceberg
ADD COLUMNS (policy_status string);

-- query the table as it looked before the column existed
SELECT * FROM curated.insurance_claims_iceberg
FOR TIMESTAMP AS OF TIMESTAMP '2026-09-01 00:00:00 UTC';
