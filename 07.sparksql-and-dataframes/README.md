# Spark DataFrame, SQL and Iceberg lab

Use an Onyxia `vscode-pyspark` service limited to 4 CPU and 4000 MiB memory.
Verify the installed Spark, Scala and `hadoop-aws` versions, then set
`ICEBERG_PACKAGE` to the matching Iceberg runtime artifact if it differs from
the course's Spark 4.1 / Scala 2.13 default. Maven Central must be reachable.
From the existing uv project directory:

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
python ../../07.sparksql-and-dataframes/lab_silver.py
uv run python ../../07.sparksql-and-dataframes/compare_silver.py
```

The script reads bronze CSVs with multiline parsing, implements users and
orders transformations using both DataFrames and SQL, replays three orders to
check window deduplication, writes Spark Parquet directories, reads them back,
then creates Iceberg tables and displays snapshot and file metadata. The
comparison reads the dbt single-file outputs and Spark part files with DuckDB.
Spark stores IDs as strings while dbt stores them as UUIDs. The dbt export
files are `users.parquet` and `orders.parquet`; these are the names specified
in the dbt lab's export section, despite the Spark lab showing `stg_` paths.

For local verification without Onyxia, provide `--bronze-root`,
`--silver-root` and `--skip-iceberg`, then set `LAB_SILVER_ROOT` and
`LAB_SPARK_SILVER_ROOT` for the comparison. A local Iceberg run additionally
requires `--warehouse-root` and a compatible `ICEBERG_PACKAGE`.

After the Onyxia Iceberg run, collect actual metadata into this repository:

```bash
aws s3 --profile default sync "s3://$LAB_BUCKET_NAME/warehouse/" ./warehouse/ \
  --exclude '*' --include 'silver/*/metadata/*'
git add ./warehouse/silver/orders/metadata ./warehouse/silver/users/metadata
git diff --cached --check
git commit -m "feat(spark-lab): add Iceberg table metadata"
git push
```

Inspect the staged metadata before committing; data part files and generated
local outputs do not belong in Git. Run the sync from the Git repository root.
