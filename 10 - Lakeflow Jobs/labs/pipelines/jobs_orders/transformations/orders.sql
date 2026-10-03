-- Section 10 · 10-L3 · the pipeline run by the PIPELINE TASK of the job "10-L3 Land and Pipeline"
-- Configuration (set by create_or_update_pipeline10() or in the pipeline settings):  inbox_path = /Volumes/<catalog>/shopwave/raw/lab10/inbox

-- Bronze: every new file of the inbox, exactly once (streaming table = incremental)
CREATE OR REFRESH STREAMING TABLE pl10_bronze_orders
COMMENT 'Section 10: July orders from the lab10 inbox, loaded by a pipeline task'
AS SELECT order_id, order_timestamp, customer_id, quantity, total, items,
          _metadata.file_name AS source_file
   FROM STREAM read_files('${inbox_path}', format => 'json',
                          schemaHints => 'order_timestamp BIGINT, quantity INT, total DOUBLE');

-- Gold: revenue per day (materialized view = recomputed/incrementally refreshed on every pipeline update)
CREATE OR REFRESH MATERIALIZED VIEW pl10_daily_revenue
COMMENT 'Section 10: daily revenue of valid orders, refreshed by the pipeline task'
AS SELECT to_date(timestamp_seconds(order_timestamp)) AS order_date,
          count(*)                                    AS orders,
          round(sum(total), 2)                        AS revenue
   FROM (SELECT DISTINCT order_id, order_timestamp, quantity, total FROM pl10_bronze_orders)   -- drop exact duplicates
   WHERE quantity > 0
   GROUP BY 1;
