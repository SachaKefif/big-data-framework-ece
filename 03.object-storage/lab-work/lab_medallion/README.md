# Bronze to silver with dbt

This project reads bronze users and orders CSVs and builds typed silver views
in DuckDB. `LAB_BUCKET_NAME` selects the Onyxia S3 bucket. For local
verification, `LAB_BRONZE_ROOT` may point at a directory with the same CSVs.

From this directory, run `uv run dbt debug`, then `uv run dbt build`.
