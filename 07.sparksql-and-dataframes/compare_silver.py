"""Compare dbt and Spark silver Parquet outputs with DuckDB."""

import os
from pathlib import Path

import duckdb


def root(env_name: str, default: str) -> str:
    value = os.environ.get(env_name, default)
    return value.rstrip("/") if "://" in value else Path(value).resolve().as_posix()


def main() -> None:
    bucket = os.environ.get("LAB_BUCKET_NAME", "")
    dbt_root = root("LAB_SILVER_ROOT", f"s3://{bucket}/silver/dbt")
    spark_root = root("LAB_SPARK_SILVER_ROOT", f"s3://{bucket}/silver/spark")
    with duckdb.connect() as con:
        for name in ("users", "orders"):
            paths = {
                "dbt": f"{dbt_root}/{name}.parquet",
                "spark": f"{spark_root}/{name}.parquet/*.parquet",
            }
            results = {}
            for engine, path in paths.items():
                if name == "orders":
                    results[engine] = con.execute(
                        "SELECT count(*), sum(quantity) FROM read_parquet(?)", [path]
                    ).fetchone()
                else:
                    results[engine] = con.execute(
                        "SELECT count(*) FROM read_parquet(?)", [path]
                    ).fetchone()
                schema = con.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)", [path]
                ).fetchall()
                print(name, engine, "rows/quantity", results[engine])
                print(name, engine, "schema", [(row[0], row[1]) for row in schema])
            if results["dbt"] != results["spark"]:
                raise AssertionError(f"{name} outputs differ: {results}")
    print("dbt and Spark silver totals agree")


if __name__ == "__main__":
    main()
