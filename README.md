# Fingrid data platform — AWS implementation

A metadata-driven medallion lakehouse over [Fingrid's open API](https://data.fingrid.fi/en)
— Finland's national grid operator. Electricity consumption, wind and solar generation,
ingested incrementally and modelled into a conformed star schema.

This is the AWS build. The same platform was built first on Azure Data Factory, in
[Data_Engineering_Fingrid](https://github.com/Einsuomi/Data_Engineering_Fingrid).

## What's in here

| Path | What it does |
|---|---|
| `src/notebooks/` | Control-table ingestion. A configuration task seeds the table, a lookup reads the active datasets, and a for-each fans a paginated, rate-limit-aware API pull across them into an S3 landing zone. Incremental by watermark: each run reads the last-loaded timestamp and writes the new one back, so re-runs are idempotent. |
| `src/pipelines/DLT_Pipeline/` | The medallion as Delta Live Tables. Bronze reads the landing zone with Auto Loader and applies `@dlt.expect_all_or_drop` quality expectations; silver flattens and types per dataset; gold builds the facts and dimensions with Auto CDC (SCD-1). |
| `databricks.yml`, `resources/` | The Databricks Asset Bundle — job, DLT pipeline and variables, deployed per target. `dev` and `test` are configured; `prod` is a placeholder that was never filled in. |
| `.github/workflows/` | Deploys and runs the bundle. Manual dispatch only — there is no push or pull-request trigger and no approval gate. |
| `terraform/` | The AWS underneath. See below. |

## Terraform — what it is, and what it isn't

About forty lines.

- `main.tf` creates one S3 bucket per medallion layer with `for_each`, named per environment.
- `backend.tf` is the part worth reading: remote state in S3, DynamoDB state locking,
  encryption at rest, and a separate state file per Terraform workspace, so dev, test and
  prod never share state.
- The Databricks workspace resource is written but **commented out** — it needs pre-existing
  network and credential ARNs that were never built.
- There are **no Unity Catalog resources here.** Catalogs and schemas are created from the
  Databricks side, not from Terraform.
- It was run locally, never through CI.

The split between the two tools is deliberate. The bundle manages what lives inside the
Databricks workspace; Terraform manages the cloud underneath it. Only Terraform has state
and drift to look after, which is why it gets a remote backend and a lock table.

## Status

The AWS account behind this project has been closed, so nothing here runs today and the
workflow history is historical. The repository is the artifact.
