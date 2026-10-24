# ECE Big Data labs

Open the lab files from this repository root:

| Lab | Location |
| --- | --- |
| 1. uv project | [`03.object-storage/lab-work/`](03.object-storage/lab-work/) |
| 2. S3 and Kubernetes | [`s3-commands.md`](03.object-storage/lab-work/s3-commands.md) and [`k8s/`](03.object-storage/lab-work/k8s/) |
| 3. DuckDB | [`sql/`](03.object-storage/lab-work/sql/) and [`orders_report.py`](03.object-storage/lab-work/src/object_storage_labs/orders_report.py) |
| 4. dbt | [`lab_medallion/`](03.object-storage/lab-work/lab_medallion/) — models are under [`models/silver/`](03.object-storage/lab-work/lab_medallion/models/silver/) |
| 5. Spark RDD | [`06.spark-introduction/lab_rdd.py`](06.spark-introduction/lab_rdd.py) |
| 6. Spark DataFrame and Iceberg | [`07.sparksql-and-dataframes/lab_silver.py`](07.sparksql-and-dataframes/lab_silver.py) |

The lab numbers above follow the task list. The course directory names use their
module numbers, so lab 4 (dbt) is not a top-level `04` directory. The dbt
project lives inside `03.object-storage/lab-work` so `uv run` finds the parent
`pyproject.toml` and environment. VS Code's Compact Folders setting may display
`03.object-storage\lab-work` as one folder in Explorer; expand it to see
`lab_medallion`.

See [`LAB_STATUS.md`](LAB_STATUS.md) for verification results and the Onyxia
steps that still require access to the course environment.
