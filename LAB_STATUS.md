# ECE Big Data lab status — 2026-10-24

Official instructions: [uv](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-1-uv.md), [S3](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-2-s3.md), [DuckDB](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/04.sql-analytics/lab-duckdb.md), [dbt](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/05.etl-elt-and-medallion-architecture/lab-dbt.md), [RDD](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/06.spark-introduction/lab-pyspark-rdd.md), and [DataFrame/Iceberg](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/07.sparksql-and-dataframes/lab-pyspark-silver.md). The Kubernetes ConfigMap/Secret and Job prerequisites were also reviewed.

## Environment and submission

- This is a local Windows checkout, not Onyxia. The original student repository and its `main` branch were reused; no existing source files were replaced wholesale and Git history was not rewritten.
- Git identity and the student's own remote are configured: <https://github.com/SachaKefif/big-data-framework-ece>. Commits listed below are on `main` and pushed to that remote.
- Python 3.13.5, uv 0.10.7, Git, Java 21, and a Windows PySpark 4.0.1 installation are available. The DuckDB 2.0.0 Python package and dbt Core 1.12.6 with dbt-duckdb 1.11.0 were installed into the existing uv project. The Windows PySpark worker crashed, so Spark jobs were verified in Ubuntu/WSL with Java 17 and the installed PySpark 4.0.1 runtime.
- AWS CLI, kubectl, an AWS profile, S3 credentials, endpoint and bucket name are absent from this session. No assigned bucket can be checked or modified here. No `InternalError` was observed because no authenticated S3 request could be made; no S3 success is claimed.
- Generated CSVs, Parquet files, DuckDB databases, dbt `target/` and `logs/`, virtual environments, and secrets are ignored or held outside the repository. No screenshots or results were fabricated.

## 1. Initial uv project

- **Requirements:** uv package, README and entry points, reproducible users/orders generation, dependency lock, project initialization and Git commits.
- **Completed:** Reused the existing `object-storage-labs` package and its generators; added `.python-version` and integration tests. The generators produce user-linked orders and valid multiline CSV addresses.
- **Files:** `03.object-storage/lab-work/{.python-version,pyproject.toml,uv.lock,README.md,src/object_storage_labs/{serialize.py,dataset_users.py,dataset_orders.py},tests/test_generators.py}`.
- **Commands executed:** `uv sync --locked`; generator help and sample runs; `uv run python -m unittest discover -s tests -v`.
- **Test results:** Dependency sync succeeded; 2 tests passed; generated default CSVs contained 50 users and 2,242 orders. The count differs from the course's sample output because the existing generator and installed Faker version produce different records; referential and uniqueness tests passed.
- **Remaining issues:** The pre-existing work was committed as `feat: add object storage labs`, not the three separate commit messages shown in the uv lab. Rewriting or inventing earlier commits would misrepresent its history. Onyxia bootstrap was not performed from this machine.
- **Git commit SHA:** `5a4aea1` (pre-existing), `a385861` (Python pin and tests).
- **Submission status:** Source pushed; local verification passed. Historical commit structure remains different from the course example.

## 2. S3 and Kubernetes ingestion

- **Requirements:** Profile/bucket check; PUT, LIST, GET, move, DELETE, metadata, presigned URL, multipart, versioning, lifecycle, policy/ACL, consistency; bronze CSV upload; ConfigMaps, Secret and ingestion Job.
- **Completed:** Existing Job manifest retained. Expanded the runbook with a small upload/download checksum test and object operation commands; documented how to follow the official administrative demonstrations and record unsupported operations. Bronze datasets were generated locally.
- **Files:** `03.object-storage/lab-work/{s3-commands.md,k8s/job-upload-bronze.yaml}`.
- **Commands executed:** Local dataset generation only. No AWS CLI or kubectl command could be executed here.
- **Test results:** Local CSV integrity passed; bucket access, object round trip and Job completion **not verified**.
- **Remaining issues:** Run the smoke test and every live demonstration in Onyxia, then create the ConfigMaps/Secret, run the Job, verify both bronze objects, and remove the Kubernetes objects. Some bucket administration calls may return `AccessDenied` or `NotImplemented`, as the lab anticipates. Do not record an S3 outcome before running it.
- **Git commit SHA:** `5a4aea1` (pre-existing manifest), `378f07a` (runbook). The official S3 lab has no dedicated Git commit section.
- **Submission status:** Preparation pushed; mandatory live work pending.

## 3. DuckDB analytics

- **Requirements:** DuckDB and S3 configuration; bronze SQL inspection, data checks, aggregations, joins, windows and pivot; Parquet conversion, partitioning and large-file comparison; Python report; required commit.
- **Completed:** Added DuckDB dependency, SQL workbooks and parameterized `orders-report` entry point. The workbooks run over local CSVs and support S3 roots in Onyxia. Added the large CSV versus Parquet workbook.
- **Files:** `03.object-storage/lab-work/{sql/analytics.sql,sql/large_benchmark.sql,src/object_storage_labs/orders_report.py,README.md,pyproject.toml,uv.lock,.gitignore}`.
- **Commands executed:** `uv add duckdb`; `uv run orders-report` and `-p cookie` with `LAB_BRONZE_ROOT`; Python DuckDB execution of both SQL workbooks; large dataset generation with `count_users=5000`.
- **Test results:** Small workbook and report succeeded on 50 users/2,242 orders; Parquet readback returned 2,242 orders. The large local CSV contained 250,147 orders and occupied 27,558,916 bytes; its Parquet file occupied 11,166,475 bytes and had 3 row groups. The SQL workbooks executed successfully.
- **Remaining issues:** Run the same queries, report and large-file transfer measurements against the assigned S3 bucket in Onyxia; the DuckDB CLI and persistent Onyxia secret are not available here.
- **Git commit SHA:** `aa2f611` (required `feat: orders report with duckdb`), `4e64107` (large benchmark).
- **Submission status:** Code pushed and locally verified; S3 verification pending.

## 4. dbt bronze to silver

- **Requirements:** dbt-duckdb project and profile, S3 bronze sources, staging views, typing/cleaning/deduplication, schemas and tests, `dbt build`, Parquet export, documentation and four course commits.
- **Completed:** Created `lab_medallion` with the required names; added local bronze override without changing the S3 default; supplied a USPS-backed ephemeral `states` reference missing from the lab instructions; added generic and singular tests and Parquet export for both tables.
- **Files:** `03.object-storage/lab-work/lab_medallion/{dbt_project.yml,profiles.yml,README.md,export_silver.py,models/bronze/{sources.yml,states.sql},models/silver/{stg_users.sql,stg_orders.sql,silver.yml},tests/{assert_orders_have_users.sql,assert_orders_after_user_birth.sql}}` plus the uv dependency files.
- **Commands executed:** `uv add dbt-duckdb`; `uv run dbt debug`; `dbt show` on the bronze users source; `dbt run --select silver`; `dbt build --select silver`; full `dbt build`; `dbt docs generate --static`; local Parquet export/readback.
- **Test results:** `dbt debug` passed; both views built; full build: 16 PASS, 1 WARN, 0 ERROR across 17 steps. The warning is 29 orders dated before their user's birthdate, retained as the lab directs. Parquet exports read back 50 users and 2,242 orders. Generated docs succeeded.
- **Remaining issues:** Rebuild against Onyxia S3 and export both Parquet objects to `silver/dbt/`. The local warning count is dataset-specific. The course references `ref('states')` but supplies no relation; the added ephemeral reference makes the test runnable.
- **Git commit SHA:** `3b0ecc6`, `9b005a4`, `51e313b`, `9d441d1` (all four course messages).
- **Submission status:** Code pushed and locally verified; S3 build and exports pending.

## 5. Spark RDD

- **Requirements:** Local bronze download; partitions/laziness/lineage, multiline address limitation, parsing, `reduceByKey` versus `groupByKey`, deduplication and user join.
- **Completed:** Added an executable walkthrough and explanations. Assertions compare both aggregations and print actual lineage and join results.
- **Files:** `06.spark-introduction/{lab_rdd.py,README.md}`.
- **Commands executed:** `python lab_rdd.py --master local[2]` in Ubuntu/WSL against the generated CSVs.
- **Test results:** Exit 0; 101 physical user lines became 50 user records; 2,242 orders before and after deduplication; both aggregation methods agreed; join returned 2,242 rows and 0 unmatched orders.
- **Remaining issues:** Download actual bronze objects in Onyxia and inspect the Spark UI shuffle metrics there. The course does not specify an RDD commit message.
- **Git commit SHA:** `757ed7e`.
- **Submission status:** Code pushed and locally verified; Onyxia data/UI verification pending.

## 6. Spark DataFrame, SQL and Iceberg

- **Requirements:** Onyxia S3A session within 4 CPU/4000 MiB limits, bronze reads, equivalent DataFrame and SQL silver rules, replay deduplication, Parquet and Iceberg writes/metadata, dbt comparison and metadata commit.
- **Completed:** Added DataFrame and SQL versions for users and orders, three-row replay/deduplication, Parquet readback, Iceberg table creation and metadata queries, and a DuckDB comparison script. Local mode supports verification without S3.
- **Files:** `07.sparksql-and-dataframes/{lab_silver.py,compare_silver.py,README.md}`.
- **Commands executed:** WSL `python lab_silver.py --bronze-root ... --silver-root ... --skip-iceberg --master local[2]`; a second run with a local warehouse and `ICEBERG_PACKAGE=org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0`; `python compare_silver.py`.
- **Test results:** Both runs exited 0. Replaying 3 orders yielded 2,245 rows, deduplicated to 2,242. DataFrame and SQL row sets agreed. Spark Parquet and local Iceberg tables read back 50 users/2,242 orders; Iceberg snapshot and file metadata were queried. dbt and Spark outputs matched at 50 users, 2,242 orders and 6,703 units. Spark IDs are `VARCHAR` versus dbt `UUID`; Spark timestamp is `TIMESTAMP` versus dbt `TIMESTAMP WITH TIME ZONE`.
- **Remaining issues:** Run against Onyxia S3 with its installed Spark/Scala-compatible Iceberg package, inspect the S3 outputs, sync the actual Iceberg `metadata/` files and make the professor's required `feat(spark-lab): add Iceberg table metadata` commit. Local warehouse metadata was deliberately not presented as S3 metadata.
- **Git commit SHA:** `b0f1e85` (implementation); required Iceberg metadata commit pending.
- **Submission status:** Code pushed and locally verified, including local Iceberg; Onyxia S3 and metadata submission pending.
