# Bronze to silver with dbt

This project reads bronze users and orders CSVs and builds typed silver views
in DuckDB. `LAB_BUCKET_NAME` selects the Onyxia S3 bucket. For local
verification, `LAB_BRONZE_ROOT` may point at a directory with the same CSVs.

From this directory, run `uv run dbt debug`, then `uv run dbt build`.
The expected Faker birthdate anomaly is reported as a warning by
`assert_orders_after_user_birth`. The `states` ephemeral reference supplies
the USPS state and military codes needed by the lab's relationship test.

Run `uv run dbt docs generate --static` for lineage and documentation. After
building, run `uv run python export_silver.py` to write `users.parquet` and
`orders.parquet` to `s3://$LAB_BUCKET_NAME/silver/dbt/`. Set
`LAB_SILVER_ROOT` to a local directory to verify the export without S3.
