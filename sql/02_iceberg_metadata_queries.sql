-- Every Iceberg table exposes a set of read-only pseudo-tables that let you
-- inspect its metadata with plain SQL, instead of reaching into the raw
-- JSON/Avro files sitting in the metadata/ folder yourself.
-- Run each in the Athena console (workgroup: analytics-prod).

-- one row per partition, with record/file counts and size
SELECT * FROM "curated"."insurance_claims_iceberg$partitions";

-- one row per data file: path, partition value, size, column-level stats
SELECT * FROM "curated"."insurance_claims_iceberg$files";

-- one row per manifest: which files it groups, and for which snapshot
SELECT * FROM "curated"."insurance_claims_iceberg$manifests";

-- the table's full write history — every snapshot, its timestamp, and its parent
SELECT * FROM "curated"."insurance_claims_iceberg$snapshots";
