-- Set large_root in init.sql to a local directory or s3://<bucket>/large.
-- Generate the source with: uv run dataset-orders -u 5000 -o csv > orders_large.csv
-- In Onyxia upload that file as large/orders.csv before running this workbook.
SET TimeZone = 'UTC';
COPY (
    FROM read_csv(getvariable('large_root') || '/orders.csv', strict_mode = false)
) TO (getvariable('large_root') || '/orders.parquet')
    (FORMAT parquet, OVERWRITE_OR_IGNORE true);

SELECT count(*) AS orders
FROM read_parquet(getvariable('large_root') || '/orders.parquet');
SELECT row_group_id, row_group_num_rows, stats_min, stats_max
FROM parquet_metadata(getvariable('large_root') || '/orders.parquet')
WHERE path_in_schema = 'date';

-- Run each EXPLAIN ANALYZE in a fresh DuckDB process for uncached S3
-- transfer measurements; the statements are grouped here for convenience.
SET enable_external_file_cache = false;
EXPLAIN ANALYZE
SELECT product, sum(quantity)
FROM read_csv(getvariable('large_root') || '/orders.csv', strict_mode = false)
GROUP BY product;
EXPLAIN ANALYZE
SELECT product, sum(quantity)
FROM read_parquet(getvariable('large_root') || '/orders.parquet')
GROUP BY product;
EXPLAIN ANALYZE
SELECT count(*)
FROM read_parquet(getvariable('large_root') || '/orders.parquet')
WHERE date >= '2100-01-01';
