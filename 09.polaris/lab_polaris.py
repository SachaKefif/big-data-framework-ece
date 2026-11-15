"""Register and govern the module 7 Iceberg tables through Apache Polaris."""

import argparse
import os
import re
import uuid

from pyspark.sql import SparkSession


def spark_session(principal: str, master: str) -> SparkSession:
    bucket = os.environ.get("LAB_BUCKET_NAME") or os.environ.get("KUBERNETES_NAMESPACE")
    if not bucket:
        raise RuntimeError("LAB_BUCKET_NAME or KUBERNETES_NAMESPACE is required")
    endpoint = os.environ["AWS_S3_ENDPOINT"]
    if not endpoint.startswith(("http://", "https://")):
        endpoint = "https://" + endpoint
    polaris_uri = os.environ.get("POLARIS_URI", "http://polaris:8181").rstrip("/")
    catalog = os.environ.get("POLARIS_CATALOG", bucket)
    credential = os.environ[f"POLARIS_{principal.upper()}_CREDENTIAL"]
    spark = (
        SparkSession.builder.appName(f"lab-polaris-{principal}").master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", os.environ.get(
            "ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "us-east-1"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"s3a://{bucket}/warehouse")
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", f"{polaris_uri}/api/catalog")
        .config("spark.sql.catalog.polaris.warehouse", catalog)
        .config("spark.sql.catalog.polaris.credential", credential)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def latest_metadata(spark: SparkSession, bucket: str) -> str:
    """Find the latest Hadoop-catalog metadata JSON without hardcoding v1."""
    path = spark._jvm.org.apache.hadoop.fs.Path(
        f"s3a://{bucket}/warehouse/silver/orders/metadata"
    )
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    versions = []
    for item in fs.listStatus(path):
        name = item.getPath().getName()
        match = re.fullmatch(r"v(\d+)\.metadata\.json", name)
        if match:
            versions.append((int(match.group(1)), item.getPath().toString()))
    if not versions:
        raise RuntimeError("No vN.metadata.json found for lab.silver.orders")
    return max(versions)[1]


def table_exists(spark: SparkSession, name: str) -> bool:
    return any(row["tableName"] == name for row in spark.sql("SHOW TABLES IN polaris.silver").collect())


def dev_workflow(spark: SparkSession, bucket: str) -> None:
    spark.sql("SHOW NAMESPACES IN lab").show()
    spark.sql("SHOW NAMESPACES IN polaris").show()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    source_count = spark.table("lab.silver.orders").count()
    if not table_exists(spark, "orders"):
        metadata = latest_metadata(spark, bucket)
        escaped = metadata.replace("'", "''")
        spark.sql(
            "CALL polaris.system.register_table("
            f"table => 'silver.orders', metadata_file => '{escaped}')"
        ).show(truncate=False)
    registered_count = spark.table("polaris.silver.orders").count()
    assert source_count == registered_count, (source_count, registered_count)
    print(f"Registered orders: {registered_count} rows")
    spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)

    if not table_exists(spark, "users"):
        spark.table("lab.silver.users").writeTo("polaris.silver.users").create()
    users_count = spark.table("polaris.silver.users").count()
    assert users_count == spark.table("lab.silver.users").count()
    columns = spark.table("polaris.silver.users").columns
    # Module 8 may already have added country to the Hadoop source table.
    evolution_column = "country" if "country" not in columns else "polaris_tag"
    if evolution_column not in columns:
        spark.sql(f"ALTER TABLE polaris.silver.users ADD COLUMN {evolution_column} STRING")
    spark.sql("DESCRIBE polaris.silver.users").show(truncate=False)
    print(f"Polaris users: {users_count} rows")


def analyst_workflow(spark: SparkSession) -> None:
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql(
        "SELECT product, count(*) AS orders FROM polaris.silver.orders "
        "GROUP BY product ORDER BY orders DESC"
    ).show()
    before = spark.table("polaris.silver.orders").count()
    probe_id = str(uuid.uuid4())
    columns = spark.table("polaris.silver.orders").columns
    projection = ", ".join(
        f"'{probe_id}' AS order_id" if col == "order_id" else f"`{col}`"
        for col in columns
    )
    try:
        spark.sql(
            f"INSERT INTO polaris.silver.orders SELECT {projection} "
            "FROM polaris.silver.orders LIMIT 1"
        )
    except Exception as exc:
        message = str(exc)
        if not any(word in message.lower() for word in ("forbidden", "denied", "unauthorized", "403")):
            raise
        print("Analyst write denied by Polaris:", message[:500])
    else:
        # An incorrectly configured role must not leave the probe row behind.
        spark.sql(f"DELETE FROM polaris.silver.orders WHERE order_id = '{probe_id}'")
        raise AssertionError("Analyst INSERT succeeded; check Polaris role grants")
    assert spark.table("polaris.silver.orders").count() == before
    try:
        spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN analyst_probe STRING")
    except Exception as exc:
        message = str(exc)
        if not any(word in message.lower() for word in ("forbidden", "denied", "unauthorized", "403")):
            raise
        print("Analyst schema change denied by Polaris:", message[:500])
    else:
        spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN analyst_probe")
        raise AssertionError("Analyst ALTER succeeded; check Polaris role grants")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--principal", choices=("dev", "analyst"), required=True)
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    bucket = os.environ.get("LAB_BUCKET_NAME") or os.environ.get("KUBERNETES_NAMESPACE")
    spark = spark_session(args.principal, args.master)
    try:
        if args.principal == "dev":
            dev_workflow(spark, bucket)
        else:
            analyst_workflow(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
