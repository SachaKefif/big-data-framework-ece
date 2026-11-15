# ECE Big Data labs: modules 1–9

This repository contains student implementations for the
[ECE 2026 fall Big Data course](https://github.com/adaltas/ece-bigdata-2026-fall).
The course requires Onyxia Kubernetes, Ceph S3, and Polaris for live verification
of several labs. Local implementations and their limits are recorded in
[LAB_STATUS.md](LAB_STATUS.md).

| Module | Implementation |
| --- | --- |
| 1. Linux foundations | [`01.introduction-to-big-data/`](01.introduction-to-big-data/) |
| 2. Kubernetes | [`02.containerization-and-kubernetes/`](02.containerization-and-kubernetes/) |
| 3. Object storage and S3 | [`03.object-storage/lab-work/`](03.object-storage/lab-work/) |
| 4. DuckDB SQL analytics | [`03.object-storage/lab-work/sql/`](03.object-storage/lab-work/sql/) and [`orders_report.py`](03.object-storage/lab-work/src/object_storage_labs/orders_report.py) |
| 5. dbt medallion | [`03.object-storage/lab-work/lab_medallion/`](03.object-storage/lab-work/lab_medallion/) |
| 6. Spark RDD | [`06.spark-introduction/`](06.spark-introduction/) |
| 7. Spark DataFrame and SQL | [`07.sparksql-and-dataframes/`](07.sparksql-and-dataframes/) |
| 8. Iceberg | [`08.iceberg-introduction/`](08.iceberg-introduction/) |
| 9. Polaris | [`09.polaris/`](09.polaris/) |

The object storage project houses the module 4 and 5 code so `uv run` uses one
environment and dependency lock. The Spark scripts use PySpark from the Onyxia
image or a compatible local installation. The Polaris deployment helper is
intended for the Onyxia cluster.
