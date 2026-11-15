"""Inspect and optionally evolve the Iceberg tables produced by module 7."""

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def uri(value: str) -> str:
    return value.rstrip("/") if "://" in value else Path(value).resolve().as_uri()


def make_spark(master: str, warehouse: str, remote: bool) -> SparkSession:
    builder = (
        SparkSession.builder.appName("lab-iceberg-introduction").master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", os.environ.get(
            "ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", uri(warehouse))
    )
    if remote:
        endpoint = os.environ["AWS_S3_ENDPOINT"]
        if not endpoint.startswith(("http://", "https://")):
            endpoint = "https://" + endpoint
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    return builder.getOrCreate()


def explore(spark: SparkSession, table: str) -> None:
    print(f"\n{table}")
    spark.sql(f"DESCRIBE TABLE {table}").show(truncate=False)
    spark.sql(f"SELECT count(*) AS rows FROM {table}").show()
    for suffix, columns in (
        ("snapshots", "snapshot_id, operation, committed_at"),
        ("files", "file_path, file_format, record_count, file_size_in_bytes"),
        ("manifests", "path, added_data_files_count, existing_data_files_count"),
    ):
        spark.sql(f"SELECT {columns} FROM {table}.{suffix}").show(20, truncate=False)


def append_and_travel(spark: SparkSession, table: str, id_column: str, new_column: str) -> None:
    snapshots = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
    ).collect()
    if not snapshots:
        raise RuntimeError(f"{table} has no snapshot")
    before_id = snapshots[-1]["snapshot_id"]
    before_count = spark.table(table).count()
    # A distinct ID makes the new batch distinguishable from the existing rows.
    batch = spark.table(table).limit(3).withColumn(id_column, F.expr("uuid()"))
    batch.writeTo(table).append()
    after_count = spark.table(table).count()
    historical_count = spark.sql(
        f"SELECT count(*) AS n FROM {table} VERSION AS OF {before_id}"
    ).first()["n"]
    assert after_count == before_count + 3
    assert historical_count == before_count
    print(f"{table}: before={before_count}, current={after_count}, time_travel={historical_count}")

    if new_column not in spark.table(table).columns:
        spark.sql(f"ALTER TABLE {table} ADD COLUMN {new_column} STRING")
    evolved = spark.table(table).limit(1).withColumn(id_column, F.expr("uuid()"))
    evolved = evolved.withColumn(new_column, F.lit("spark"))
    evolved.writeTo(table).append()
    assert spark.table(table).count() == after_count + 1
    spark.sql(f"SELECT {new_column}, count(*) AS rows FROM {table} GROUP BY {new_column}").show()
    explore(spark, table)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-root", help="Local or s3a:// warehouse; defaults to the course bucket")
    parser.add_argument("--silver-root", help="Optional module 7 Parquet directory for comparison")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--apply", action="store_true", help="Append demo rows and evolve both tables")
    args = parser.parse_args()
    bucket = os.environ.get("LAB_BUCKET_NAME") or os.environ.get("KUBERNETES_NAMESPACE")
    if not args.warehouse_root and not bucket:
        parser.error("provide --warehouse-root or LAB_BUCKET_NAME")
    warehouse = args.warehouse_root or f"s3a://{bucket}/warehouse"
    spark = make_spark(args.master, warehouse, warehouse.startswith("s3a://"))
    spark.sparkContext.setLogLevel("ERROR")
    try:
        spark.sql("SHOW NAMESPACES IN lab").show()
        for table in ("orders", "users"):
            explore(spark, f"lab.silver.{table}")
            if args.silver_root:
                parquet = spark.read.parquet(f"{uri(args.silver_root)}/{table}.parquet")
                print(f"Plain Parquet {table}: {parquet.count()} rows; no table snapshots")
        if args.apply:
            append_and_travel(spark, "lab.silver.orders", "order_id", "source")
            append_and_travel(spark, "lab.silver.users", "user_id", "country")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
