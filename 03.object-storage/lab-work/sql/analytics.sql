-- Run after init.sql sets bronze_root and output_root, for example:
-- SET VARIABLE bronze_root = 's3://<your-bucket>/bronze';
-- SET VARIABLE output_root = 's3://<your-bucket>/analytics';
SET TimeZone = 'UTC';

-- Inspect the raw CSVs directly, including their inferred schema.
SELECT uuid, username, name, birthdate
FROM read_csv(getvariable('bronze_root') || '/users.csv', header = true, strict_mode = false)
LIMIT 3;
SELECT Delimiter, Quote, HasHeader
FROM sniff_csv(getvariable('bronze_root') || '/users.csv');
DESCRIBE FROM read_csv(getvariable('bronze_root') || '/orders.csv', strict_mode = false);
SELECT column_name, column_type, approx_unique, null_percentage
FROM (SUMMARIZE FROM read_csv(getvariable('bronze_root') || '/orders.csv', strict_mode = false));

CREATE OR REPLACE TABLE users AS
FROM read_csv(getvariable('bronze_root') || '/users.csv', strict_mode = false);
CREATE OR REPLACE TABLE orders AS
FROM read_csv(getvariable('bronze_root') || '/orders.csv', strict_mode = false);

SELECT (SELECT count(*) FROM users) AS users,
       (SELECT count(*) FROM orders) AS orders;
SELECT min(date) AS first_order, max(date) AS last_order FROM orders;

-- Data quality before analytics.
SELECT count(*) AS orphan_orders
FROM orders o ANTI JOIN users u ON o.user_uuid = u.uuid;
SELECT count(*) AS inactive_users
FROM users u ANTI JOIN orders o ON o.user_uuid = u.uuid;
SELECT uuid, count(*) AS occurrences
FROM orders GROUP BY uuid HAVING count(*) > 1;

-- Aggregations, joins, windows and pivot from the lab.
SELECT product, count(*) AS orders, sum(quantity) AS quantity,
       round(100 * sum(quantity) / sum(sum(quantity)) OVER (), 1) AS share_pct
FROM orders GROUP BY product ORDER BY quantity DESC;
SELECT date_trunc('month', date) AS month, count(*) AS orders,
       sum(quantity) AS quantity
FROM orders GROUP BY month ORDER BY month;
SELECT u.username, u.name, count(*) AS orders, sum(o.quantity) AS quantity
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL ORDER BY quantity DESC LIMIT 5;
SELECT (date_diff('year', u.birthdate, DATE '2020-01-01') // 20) * 20 AS age_group,
       count(DISTINCT u.uuid) AS users, count(*) AS orders,
       round(avg(o.quantity), 2) AS avg_quantity
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY age_group ORDER BY age_group;
WITH daily AS (
    SELECT date::DATE AS day, sum(quantity) AS quantity
    FROM orders GROUP BY day
)
SELECT day, quantity, sum(quantity) OVER (ORDER BY day) AS cumulative,
       round(avg(quantity) OVER (
           ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d
FROM daily ORDER BY day LIMIT 10;
SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
FROM orders GROUP BY month, product
QUALIFY rank() OVER (PARTITION BY month ORDER BY sum(quantity) DESC) = 1
ORDER BY month;
PIVOT (SELECT strftime(date, '%Y-%m') AS month, product, quantity FROM orders)
ON product USING sum(quantity) ORDER BY month;

-- Parquet, metadata, Hive partitioning and pruning.
COPY orders TO (getvariable('output_root') || '/orders.parquet') (FORMAT parquet);
SELECT path_in_schema, type, compression,
       total_compressed_size, total_uncompressed_size
FROM parquet_metadata(getvariable('output_root') || '/orders.parquet');
COPY orders TO (getvariable('output_root') || '/orders_by_product')
    (FORMAT parquet, PARTITION_BY (product));
SELECT file FROM glob(getvariable('output_root') || '/orders_by_product/**');
EXPLAIN ANALYZE SELECT count(*)
FROM read_parquet(getvariable('output_root') || '/orders_by_product/*/*.parquet')
WHERE product = 'cookie';
