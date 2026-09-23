-- Convert the raw CSV rows into a partitioned, ACID-transactional Iceberg
-- table sitting on Parquet in the curated zone. One statement is enough —
-- this is what makes a data lake a lakehouse.

CREATE TABLE curated.insurance_claims_iceberg
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://arun-lakehouse-curated/insurance_claims_iceberg/',
  partitioning = ARRAY['region']
) AS
SELECT ROW_NUMBER() OVER () AS policy_id, *
FROM raw.insurance_claims_raw;
