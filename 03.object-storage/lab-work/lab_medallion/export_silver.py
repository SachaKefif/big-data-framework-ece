"""Export the dbt silver views as Parquet for Spark comparison."""

import os
from pathlib import Path

import duckdb


def main() -> None:
    output_root = os.environ.get("LAB_SILVER_ROOT")
    if output_root is None:
        output_root = f"s3://{os.environ['LAB_BUCKET_NAME']}/silver/dbt"
    if "://" not in output_root:
        Path(output_root).mkdir(parents=True, exist_ok=True)

    with duckdb.connect("lab_medallion.duckdb") as con:
        for name in ("users", "orders"):
            target = f"{output_root.rstrip('/')}/{name}.parquet"
            quoted_target = target.replace("'", "''")
            con.execute(
                f"COPY (SELECT * FROM main_silver.stg_{name}) "
                f"TO '{quoted_target}' (FORMAT parquet, OVERWRITE_OR_IGNORE true)"
            )
            source_count = con.execute(
                f"SELECT count(*) FROM main_silver.stg_{name}"
            ).fetchone()[0]
            exported_count = con.execute(
                "SELECT count(*) FROM read_parquet(?)", [target]
            ).fetchone()[0]
            if source_count != exported_count:
                raise RuntimeError(f"{name} export count mismatch")
            print(f"{name}: {exported_count} rows exported to {target}")


if __name__ == "__main__":
    main()
