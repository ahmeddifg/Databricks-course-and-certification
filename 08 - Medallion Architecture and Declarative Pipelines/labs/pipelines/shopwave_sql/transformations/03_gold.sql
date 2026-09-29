-- =====================================================================================
-- 08-L2 · ShopWave orders pipeline (SQL) · GOLD
-- Materialized views: always correct, recomputed from their sources on every update
-- (incrementally when the engine can) - perfect for aggregates that BI tools read.
-- =====================================================================================

-- Temporary view: a named query used INSIDE this pipeline only - never published to the catalog.
CREATE TEMPORARY VIEW sdp_orders_unique
AS SELECT DISTINCT order_id, customer_id, country, order_date, quantity, total
FROM sdp_orders_silver;

CREATE OR REFRESH MATERIALIZED VIEW sdp_daily_revenue
COMMENT 'Orders and revenue per day (deduplicated)'
AS SELECT
  order_date,
  count(*)              AS orders,
  round(sum(total), 2)  AS revenue
FROM sdp_orders_unique
GROUP BY order_date;

CREATE OR REFRESH MATERIALIZED VIEW sdp_country_revenue
COMMENT 'Orders and revenue per customer country (unknown customers -> Unknown)'
AS SELECT
  coalesce(country, 'Unknown')  AS country,
  count(*)                      AS orders,
  round(sum(total), 2)          AS revenue
FROM sdp_orders_unique
GROUP BY coalesce(country, 'Unknown');

CREATE OR REFRESH MATERIALIZED VIEW sdp_category_revenue
COMMENT 'Units and revenue per product category'
AS SELECT
  p.category,
  sum(i.units)              AS units,
  round(sum(i.subtotal), 2) AS revenue
FROM (SELECT DISTINCT order_id, product_id, units, subtotal FROM sdp_order_items_silver) AS i
JOIN sdp_products AS p
  ON i.product_id = p.product_id
GROUP BY p.category;
