# AWS Lakehouse: Lake Formation + Athena + dbt

A CLI-only build of a governed data lakehouse on AWS: S3 for storage, the Glue Data Catalog as the metastore, Apache Iceberg as the table format, Lake Formation for governance, Athena as the query engine, and dbt on top for transformations. No console clicking, no Spark, no IaC yet — just the AWS CLI, SQL, and dbt.

I'm writing this up mainly for myself, as a record of what I actually built and the mistakes I made along the way, but it should be usable as a reference if you're trying to do the same thing. It assumes you already know what an IAM role, policy, and grant are — I'm not re-explaining AWS basics here, just what I ran and why.

## Stack

- Storage: S3, split into raw / curated / consumption zones
- Metastore: Glue Data Catalog
- Table format: Apache Iceberg
- Governance: Lake Formation
- Query engine: Athena
- Transformations: dbt, using the `dbt-athena-community` adapter

You'll need an AWS account, the CLI configured with an admin profile, `jq`, and Python 3. I'm using `export AWS_PROFILE=<your-profile>` everywhere below instead of repeating `--profile`. Swap `ACCOUNT_ID` (from `aws sts get-caller-identity`) and `REGION` for your own values throughout, and pick your own S3 prefix instead of `arun-lakehouse-*` since bucket names have to be globally unique.

The short version of the architecture: raw CSVs land in S3, get rebuilt as a partitioned Iceberg table in the curated zone, and that table sits in the Glue Catalog where Athena (and later dbt) can query it. Lake Formation sits underneath all of it as a second permissions layer — not a separate service you provision, just something that's already there in the Glue Catalog whether you configure it or not. The main thing this build proves is what people call the two-lock model: a query needs to clear an IAM check *and* a Lake Formation check, and if either one says no, the query fails. Section 3 below sets that up, breaks it on purpose, then fixes it, so it's provable instead of just described.

## 1. S3 zones and the analyst role

Three buckets, one per zone, plus a results bucket for Athena output. Then an IAM role for whoever's querying through Athena — `LakehouseAnalyst`. This role gets no S3 permissions on the data buckets at all. That's deliberate. Under Lake Formation, data access is supposed to come through a Lake Formation grant handing out temporary credentials, not straight from the IAM policy. Section 3 is where that actually gets tested.

Policy files: `iam-policies/analyst-trust-policy.json`, `iam-policies/analyst-policy.json`.

```bash
aws iam create-role --role-name LakehouseAnalyst \
  --assume-role-policy-document file://iam-policies/analyst-trust-policy.json

aws iam put-role-policy --role-name LakehouseAnalyst \
  --policy-name lakehouse-analyst-access \
  --policy-document file://iam-policies/analyst-policy.json
```

The permissions policy gives it `athena:*`, read-only Glue metadata calls, `lakeformation:GetDataAccess`, and access to the results bucket only. Nothing pointing at the raw or curated buckets. If you go looking for it and can't find it, that's the point — it's not there.

## 2. Databases, workgroup, curated Iceberg table

One Glue database per zone, an Athena workgroup so query results land somewhere, and the source CSV dropped into raw:

```bash
aws glue create-database --database-input '{"Name":"raw"}'
aws glue create-database --database-input '{"Name":"curated"}'
aws glue create-database --database-input '{"Name":"consumption"}'

aws athena create-work-group --name analytics-prod \
  --configuration '{
    "ResultConfiguration": { "OutputLocation": "s3://arun-lakehouse-athena-results/" },
    "EnforceWorkGroupConfiguration": true,
    "PublishCloudWatchMetricsEnabled": true
  }'
```

Once the raw CSV is catalogued as a table, one CTAS statement turns it into a partitioned, ACID Iceberg table on Parquet (`sql/01_curated_table_ctas.sql`):

```sql
CREATE TABLE curated.insurance_claims_iceberg
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://arun-lakehouse-curated/insurance_claims_iceberg/',
  partitioning = ARRAY['region']
) AS
SELECT ROW_NUMBER() OVER () AS policy_id, *
FROM raw.insurance_claims_raw;
```

Iceberg tables expose a handful of read-only pseudo-tables for inspecting their own metadata, which saves you from digging through the actual JSON/Avro files (`sql/02_iceberg_metadata_queries.sql`). `$partitions` and `$files` are the two I use most:

```sql
SELECT * FROM "curated"."insurance_claims_iceberg$partitions";
SELECT * FROM "curated"."insurance_claims_iceberg$files";
SELECT * FROM "curated"."insurance_claims_iceberg$manifests";
SELECT * FROM "curated"."insurance_claims_iceberg$snapshots";
```

## 3. Turning on Lake Formation, then proving it works

Nothing gets created in this step. Lake Formation is already sitting there, wired into the Glue Catalog. The thing that trips people up is that new databases inherit a legacy `IAMAllowedPrincipals` grant by default, which quietly lets any IAM principal with ordinary Glue/S3 permissions read everything, without Lake Formation ever getting a say. So the first move is turning that default off account-wide, then cleaning up the databases already created above since the setting change doesn't apply retroactively, then tagging the curated database. No grant to the analyst role yet — that's on purpose, section 3a needs the "before" state to actually mean something.

```bash
aws lakeformation put-data-lake-settings --data-lake-settings '{
    "DataLakeAdmins": [{ "DataLakePrincipalIdentifier": "arn:aws:iam::ACCOUNT_ID:user/YOUR_USERNAME" }],
    "CreateDatabaseDefaultPermissions": [],
    "CreateTableDefaultPermissions": []
  }'

# register the curated bucket so Lake Formation can vend scoped credentials for it
aws lakeformation register-resource \
  --resource-arn arn:aws:s3:::arun-lakehouse-curated \
  --use-service-linked-role

# the databases above predate the setting change, so strip their legacy grant explicitly
aws lakeformation revoke-permissions \
  --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
  --resource '{"Table": {"DatabaseName": "curated", "Name": "insurance_claims_iceberg"}}' \
  --permissions ALL

# create an LF-Tag and tag the curated database with it
aws lakeformation create-lf-tag --tag-key module --tag-values Claims Underwriting

aws lakeformation add-lf-tags-to-resource \
  --resource '{"Database": {"Name": "curated"}}' \
  --lf-tags '[{"TagKey": "module", "TagValues": ["Claims"]}]'
```

LF-Tags let you grant access to anything carrying a tag instead of naming tables one by one, so a whole team can get access to a growing set of tables without re-granting every time. Section 3a below actually uses this tag rather than just creating it.

If you skip the `IAM_ALLOWED_PRINCIPALS` revoke, every grant you set up after this point will look like it's doing nothing, because the legacy grant is already letting everyone in underneath whatever Lake Formation says. First thing I'd check if an LF permission "isn't working."

Now for the actual proof. Assume `LakehouseAnalyst` — IAM allows Athena, but there's no Lake Formation grant on the curated data yet — and run a query:

```bash
aws sts assume-role \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/LakehouseAnalyst \
  --role-session-name lf-test > analyst-creds.json

export AWS_ACCESS_KEY_ID=$(jq -r '.Credentials.AccessKeyId' analyst-creds.json)
export AWS_SECRET_ACCESS_KEY=$(jq -r '.Credentials.SecretAccessKey' analyst-creds.json)
export AWS_SESSION_TOKEN=$(jq -r '.Credentials.SessionToken' analyst-creds.json)

aws athena start-query-execution \
  --work-group analytics-prod \
  --query-execution-context Database=curated \
  --query-string "SELECT * FROM insurance_claims_iceberg LIMIT 5"
```

This should fail. `get-query-execution` shows `FAILED` with an `AccessDenied` / insufficient Lake Formation permissions message. IAM said yes, Lake Formation said no, and no won.

### 3a. Unlock it with the LF-Tag

Back on admin credentials:

```bash
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::ACCOUNT_ID:role/LakehouseAnalyst"}' \
  --permissions SELECT \
  --resource '{"LFTagPolicy": {"ResourceType": "TABLE", "Expression": [{"TagKey": "module", "TagValues": ["Claims"]}]}}'
```

Re-assume the analyst role and retry the same query. It succeeds now, and you get every row and every column back. That's the tag grant working end to end.

### 3b. Swap it for row and column filtering

An analyst usually shouldn't see everything, so replace the tag grant with a scoped one. Back on admin credentials again:

```bash
aws lakeformation revoke-permissions \
  --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::ACCOUNT_ID:role/LakehouseAnalyst"}' \
  --permissions SELECT \
  --resource '{"LFTagPolicy": {"ResourceType": "TABLE", "Expression": [{"TagKey": "module", "TagValues": ["Claims"]}]}}'

aws lakeformation create-data-cells-filter --table-data '{
    "TableCatalogId": "ACCOUNT_ID",
    "DatabaseName": "curated",
    "TableName": "insurance_claims_iceberg",
    "Name": "southeast_only",
    "RowFilter": { "FilterExpression": "region = '"'"'southeast'"'"'" },
    "ColumnWildcard": { "ExcludedColumnNames": ["smoker"] }
  }'

aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::ACCOUNT_ID:role/LakehouseAnalyst"}' \
  --permissions SELECT \
  --resource '{"DataCellsFilter": {"TableCatalogId": "ACCOUNT_ID", "DatabaseName": "curated", "TableName": "insurance_claims_iceberg", "Name": "southeast_only"}}'
```

I revoke the tag grant before adding the filter grant because Lake Formation doesn't intersect grants — if a principal holds both an unfiltered grant and a filtered one on the same table, the broader grant wins and the filter just gets ignored, silently. So it's one or the other per principal per table, never both.

Re-assume the analyst role one more time and rerun the query. Now it succeeds, but every row comes back `region = southeast` and there's no `smoker` column at all. IAM allowed the call, Lake Formation vended scoped and filtered access, and Athena never touched the raw table directly.

## 4. Iceberg operations

Back on admin credentials, against `curated.insurance_claims_iceberg`. These are the operations that make it a lakehouse table instead of a plain file sitting in S3. Full statements in `sql/03_schema_evolution_time_travel.sql`, `sql/04_merge_into_upsert.sql`, and `sql/05_optimize_vacuum.sql`.

Schema evolution and time travel, no table rewrite needed:

```sql
ALTER TABLE curated.insurance_claims_iceberg
ADD COLUMNS (policy_status string);

SELECT * FROM curated.insurance_claims_iceberg
FOR TIMESTAMP AS OF TIMESTAMP '2026-09-01 00:00:00 UTC';
```

`MERGE INTO`, an upsert without rewriting the whole table:

```sql
MERGE INTO curated.insurance_claims_iceberg t
USING curated.claims_updates s ON t.policy_id = s.policy_id
WHEN MATCHED THEN UPDATE SET policy_status = s.policy_status
WHEN NOT MATCHED THEN INSERT (policy_id, policy_status)
  VALUES (s.policy_id, s.policy_status);
```

Maintenance:

```sql
OPTIMIZE curated.insurance_claims_iceberg REWRITE DATA USING BIN_PACK;
VACUUM curated.insurance_claims_iceberg;
```

`VACUUM` doesn't delete everything old on every run. Athena's defaults, if you don't set your own, keep any snapshot newer than 5 days and always keep at least the most recent one. You can set your own thresholds:

```sql
ALTER TABLE curated.insurance_claims_iceberg SET TBLPROPERTIES (
  'vacuum_max_snapshot_age_seconds'='2592000',
  'vacuum_min_snapshots_to_keep'='5'
);
```

Longer retention gives you a bigger time-travel and rollback window at the cost of more S3 storage sitting around as superseded data. Shorter retention keeps storage cheap but means `FOR TIMESTAMP AS OF` against anything past the cutoff won't find a matching snapshot anymore.

## 5. dbt on top

The CTAS and `MERGE INTO` above are correct SQL, but nothing tracks that `insurance_claims_dbt` depends on the raw table, nothing tests that `policy_id` stays unique after a merge, and there's no documentation beyond this file. dbt is what I layered on top to get dependency tracking, tests, and generated docs, without adding a new engine — every model still compiles down to SQL that runs through Athena. Full project is in `dbt/`.

```bash
pip install dbt-core dbt-athena-community
```

dbt gets its own IAM role, since `LakehouseAnalyst` is deliberately read-only. Policy files: `iam-policies/dbt-trust-policy.json`, `iam-policies/dbt-policy.json`.

```bash
aws iam create-role --role-name LakehouseDbt \
  --assume-role-policy-document file://iam-policies/dbt-trust-policy.json

aws iam put-role-policy --role-name LakehouseDbt \
  --policy-name lakehouse-dbt-access \
  --policy-document file://iam-policies/dbt-policy.json

aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::ACCOUNT_ID:role/LakehouseDbt"}' \
  --permissions CREATE_TABLE ALTER DESCRIBE \
  --resource '{"Database": {"Name": "curated"}}'
```

`CREATE_TABLE` on a database makes you the owner of anything you create in it, so this single grant covers every table dbt is about to build.

```bash
aws sts assume-role \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/LakehouseDbt \
  --role-session-name dbt-run > dbt-creds.json

export AWS_ACCESS_KEY_ID=$(jq -r '.Credentials.AccessKeyId' dbt-creds.json)
export AWS_SECRET_ACCESS_KEY=$(jq -r '.Credentials.SecretAccessKey' dbt-creds.json)
export AWS_SESSION_TOKEN=$(jq -r '.Credentials.SessionToken' dbt-creds.json)
```

Copy `dbt/profiles.yml.example` to `~/.dbt/profiles.yml` and fill in `REGION` and your bucket names. No access key goes in that file — dbt just uses whatever's already exported in the shell. Run `dbt debug` before writing any models; it should print `All checks passed!`.

Models, tests, and a seed live in `dbt/models/staging/stg_insurance_claims.sql`, `dbt/models/marts/insurance_claims_dbt.sql`, and `dbt/models/marts/insurance_claims_status.sql`:

```bash
dbt run --select +insurance_claims_dbt
dbt test --select insurance_claims_dbt

dbt seed
dbt run --select insurance_claims_status
dbt run --select insurance_claims_status
```

That leading `+` in `+insurance_claims_dbt` matters more than it looks like it should. `dbt run --select insurance_claims_dbt` on its own only builds that one model — dbt doesn't automatically pull in a model's `ref()`'d dependencies just because they're referenced. Without `stg_insurance_claims` built first, the compiled SQL points at a view that doesn't exist yet and Athena fails with a table-not-found error, which looks like an Athena problem and isn't. The leading `+` means "this model plus everything upstream of it." I ran into this directly — asked myself when `stg_insurance_claims.sql` actually got called, realized it hadn't been, and had to fix the command.

The second `insurance_claims_status` run above merges instead of rebuilding — that's the whole point of running it twice. One thing worth checking first: the `merge` incremental strategy only works transactionally on Iceberg tables running Athena engine v3. `aws athena get-work-group --work-group analytics-prod --query 'WorkGroup.Configuration.EngineVersion'` tells you which one your workgroup is on; new workgroups default to v3.

```bash
dbt docs generate
dbt docs serve --port 8081
```

This opens a local site with the dependency graph built entirely from the `ref()` and `source()` calls in the model files: `insurance_claims_raw → stg_insurance_claims → insurance_claims_dbt → insurance_claims_status`, plus the `policy_closures` seed feeding into the last one. Nothing about that order is declared anywhere else in the project — dbt derives it from the references alone.

## Things that actually went wrong while building this

- `aws lakeformation list-permissions` needs `--resource` whenever `--principal` is set, e.g. `--resource '{"Database": {"Name": "curated"}}'`. Most other `list-*` commands don't require that, so it's easy to forget.
- A missing `s3:GetBucketLocation` on the analyst or dbt policy produces an Athena failure that reads like a permissions problem somewhere else entirely. It needs to sit alongside `GetObject`/`PutObject`/`ListBucket` on the results bucket.
- `python3 -m dbt` does not work. `dbt-core` doesn't ship a `__main__.py`, so this fails with `No module named dbt.__main__`. If `dbt` isn't found after installing, it's almost always a PATH issue from installing outside a virtualenv. `python3 -m venv .venv && source .venv/bin/activate`, confirm `which python3` points into the venv, then just use `dbt` directly.

## Cleanup

None of this bills continuously, but leftover roles, buckets, and grants have a way of turning into a monthly charge nobody notices for a while.

```bash
# Lake Formation first, or the deletes below can fail on references that still exist
aws lakeformation revoke-permissions \
  --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::ACCOUNT_ID:role/LakehouseAnalyst"}' \
  --permissions SELECT \
  --resource '{"DataCellsFilter": {"TableCatalogId": "ACCOUNT_ID", "DatabaseName": "curated", "TableName": "insurance_claims_iceberg", "Name": "southeast_only"}}'

aws lakeformation delete-data-cells-filter \
  --table-catalog-id ACCOUNT_ID --database-name curated \
  --table-name insurance_claims_iceberg --name southeast_only

aws lakeformation revoke-permissions \
  --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::ACCOUNT_ID:role/LakehouseDbt"}' \
  --permissions CREATE_TABLE ALTER DESCRIBE \
  --resource '{"Database": {"Name": "curated"}}'

aws lakeformation remove-lf-tags-from-resource \
  --resource '{"Database": {"Name": "curated"}}' \
  --lf-tags '[{"TagKey": "module", "TagValues": ["Claims"]}]'

aws lakeformation delete-lf-tag --tag-key module
aws lakeformation deregister-resource --resource-arn arn:aws:s3:::arun-lakehouse-curated

# Glue
aws glue delete-database --name raw
aws glue delete-database --name curated
aws glue delete-database --name consumption

# Athena
aws athena delete-work-group --work-group analytics-prod --recursive-delete-option

# IAM
aws iam delete-role-policy --role-name LakehouseAnalyst --policy-name lakehouse-analyst-access
aws iam delete-role --role-name LakehouseAnalyst
aws iam delete-role-policy --role-name LakehouseDbt --policy-name lakehouse-dbt-access
aws iam delete-role --role-name LakehouseDbt

# S3, empty each bucket before deleting it
for b in raw curated consumption athena-results; do
  aws s3 rm s3://arun-lakehouse-$b --recursive
  aws s3 rb s3://arun-lakehouse-$b
done
```

## What's next

Deliberately stopped here to keep this a clean, CLI-only build. Two things are coming as separate additions rather than a rewrite:

- Redshift Spectrum, to show the same Lake Formation grant governing a second query engine reading the same Iceberg table through the same catalog.
- Terraform, to rebuild this same setup as code once the manual version is solid.
