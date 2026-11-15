# Module 8: Iceberg

Run module 7 first so `lab.silver.orders` and `lab.silver.users` exist. From the
repository root, inspect the tables with:

```bash
python 08.iceberg-introduction/lab_iceberg.py
```

Use `--apply` once to append three distinct rows per table, query the prior
snapshot, add `source` to orders and `country` to users, and append an evolved
row. Each `--apply` run adds four rows to each table. To compare plain Parquet,
pass `--silver-root s3a://$LAB_BUCKET_NAME/silver/spark`.

For local work, pass `--warehouse-root /path/to/warehouse` and
`--silver-root /path/to/silver/spark`. Set `ICEBERG_PACKAGE` to the artifact
matching your Spark/Scala installation. Keep the warehouse for module 9;
the upstream Iceberg lab's cleanup step conflicts with the Polaris lab's
registration prerequisite.
