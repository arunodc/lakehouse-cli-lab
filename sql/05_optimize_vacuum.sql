-- Maintenance — matters more at real scale than on a toy table, but the
-- commands and the retention tradeoff are the same either way.

OPTIMIZE curated.insurance_claims_iceberg REWRITE DATA USING BIN_PACK;
VACUUM curated.insurance_claims_iceberg;

-- VACUUM respects two retention settings. Athena's defaults, if you never
-- set your own: snapshots newer than 5 days are always kept regardless of
-- count, and at least the 1 most recent snapshot is always kept regardless
-- of age. Set your own window with table properties:

ALTER TABLE curated.insurance_claims_iceberg SET TBLPROPERTIES (
  'vacuum_max_snapshot_age_seconds'='2592000',  -- 30 days
  'vacuum_min_snapshots_to_keep'='5'
);

-- Tradeoff: a longer window means more time-travel range and a bigger audit
-- window, at the cost of more S3 storage held as superseded data/manifest
-- files. A short window keeps storage lean but FOR TIMESTAMP AS OF against
-- anything older than the cutoff simply won't find a matching snapshot.
