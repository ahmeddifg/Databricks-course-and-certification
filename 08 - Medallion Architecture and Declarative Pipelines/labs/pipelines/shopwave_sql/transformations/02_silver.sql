-- =====================================================================================
-- 08-L2 · ShopWave orders pipeline (SQL) · SILVER
-- Datasets are referenced by name - no LIVE. prefix, no paths. The pipeline works out
-- the order (bronze -> silver -> gold) from these references, not from the file names.
-- =====================================================================================

-- Streaming table fed by another streaming table (STREAM(...)) + a stream-static join with a materialized view.
-- Expectations = data-quality rules. Three possible actions:
--   EXPECT (...)                          -> warn : keep the row, count it in the metrics
--   EXPECT (...) ON VIOLATION DROP ROW    -> drop : remove the row, count it
--   EXPECT (...) ON VIOLATION FAIL UPDATE -> fail : stop the update, roll back this flow's transaction
CREATE OR REFRESH STREAMING TABLE sdp_orders_silver (
  CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT not_cancelled   EXPECT (quantity > 0)         ON VIOLATION DROP ROW,
  CONSTRAINT known_customer  EXPECT (country IS NOT NULL)
)
COMMENT 'Valid orders with a proper timestamp and the customer country (duplicates are removed in gold)'
AS SELECT
  o.order_id,
  o.customer_id,
  c.country,
  CAST(from_unixtime(o.order_timestamp) AS TIMESTAMP)  AS order_ts,     -- Unix seconds -> TIMESTAMP
  CAST(from_unixtime(o.order_timestamp) AS DATE)       AS order_date,
  o.quantity,
  o.total,
  o.items,
  o.source_file
FROM STREAM(sdp_orders_bronze) AS o
LEFT JOIN sdp_customers AS c
  ON o.customer_id = c.customer_id;

-- One row per order line: explode the items array of each new silver row.
CREATE OR REFRESH STREAMING TABLE sdp_order_items_silver
COMMENT 'Order lines (items array exploded)'
AS SELECT
  order_id,
  order_date,
  item.product_id,
  item.quantity  AS units,
  item.subtotal
FROM (SELECT order_id, order_date, explode(items) AS item FROM STREAM(sdp_orders_silver));
