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
