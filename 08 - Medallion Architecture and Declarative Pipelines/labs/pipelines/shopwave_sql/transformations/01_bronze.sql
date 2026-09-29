-- =====================================================================================
-- 08-L2 · ShopWave orders pipeline (SQL) · BRONZE
-- Lakeflow Spark Declarative Pipelines source file. It is NOT run like a notebook:
-- the pipeline reads every file in transformations/, builds the dependency graph and
-- creates/refreshes the datasets. ${dataset_path} comes from the pipeline Configuration.
-- =====================================================================================

-- Streaming table: incremental ingestion with Auto Loader (read_files + STREAM).
-- Each update reads ONLY the files that arrived since the last update (checkpoint managed for you).
CREATE OR REFRESH STREAMING TABLE sdp_orders_bronze
COMMENT 'Raw ShopWave orders - one row per JSON record, loaded incrementally from the landing folder'
AS SELECT
  *,
  _metadata.file_name  AS source_file,        -- lineage: which file did this row come from?
  current_timestamp()  AS ingested_at
FROM STREAM read_files(
  '${dataset_path}/lab08/orders',
  format      => 'json',
  schemaHints => 'order_timestamp BIGINT, quantity INT, total DOUBLE'
);

-- Materialized view: a batch source (no STREAM) that is recomputed - incrementally when possible - on every update.
CREATE OR REFRESH MATERIALIZED VIEW sdp_customers
COMMENT 'Customer dimension parsed from the raw JSON export'
AS SELECT
  customer_id,
  email,
  profile:first_name::string        AS first_name,
  profile:last_name::string         AS last_name,
  profile:address:city::string      AS city,
  profile:address:country::string   AS country
FROM read_files('${dataset_path}/customers-json', format => 'json');

CREATE OR REFRESH MATERIALIZED VIEW sdp_products
COMMENT 'Product dimension from the ; delimited CSV export'
AS SELECT product_id, title, brand, category, CAST(price AS DOUBLE) AS price
FROM read_files('${dataset_path}/products-csv', format => 'csv', header => true, sep => ';');
