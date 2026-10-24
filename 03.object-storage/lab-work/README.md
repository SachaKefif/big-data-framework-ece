# Object storage labs

This project implements Labs 1 and 2 of Part 3. It generates reproducible user
and order datasets, then provides a Kubernetes Job manifest for uploading both
datasets into an S3 bronze layer.

## Setup

```powershell
uv sync
```

## Generate datasets

```powershell
uv run dataset-users -c 50 -o csv > users.csv
uv run dataset-orders -u 50 -C 0 -c 100 -o csv > orders.csv
```

Supported output formats are `csv`, `json`, and `jsonline`.

## Upload to S3

The live S3 steps require a configured AWS CLI `default` profile, an S3 endpoint,
and a bucket name. Follow [s3-commands.md](s3-commands.md) after exporting
`S3_ENDPOINT_URL` and `LAB_BUCKET_NAME`. The Kubernetes upload manifest is in
`k8s/job-upload-bronze.yaml`.

## DuckDB analytics

The [SQL workbook](sql/analytics.sql) covers bronze CSV inspection, quality
checks, aggregations, joins, windows, pivot, and Parquet export. In Onyxia,
create an ignored `init.sql` with your bucket:

```sql
SET VARIABLE bronze_root = 's3://<your-bucket>/bronze';
SET VARIABLE output_root = 's3://<your-bucket>/analytics';
SET TimeZone = 'UTC';
```

Run `duckdb analytics.duckdb -init init.sql`, then `.read sql/analytics.sql`.
The CLI and Python package use the Onyxia DuckDB S3 secret.

The Python report queries the bronze orders CSV directly:

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
uv run orders-report
uv run orders-report -p cookie
```

For local verification, set `LAB_BRONZE_ROOT` to a directory containing
`users.csv` and `orders.csv` before running `orders-report`.
