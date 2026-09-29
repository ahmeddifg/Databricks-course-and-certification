-- =====================================================================================
-- 08-L4 · SOLUTION (Tasks 1-3) · ShopWave product ratings pipeline
-- =====================================================================================

-- Task 1 · BRONZE
CREATE OR REFRESH STREAMING TABLE ch08_ratings_bronze
COMMENT 'Raw product ratings from the mobile app, loaded incrementally'
AS SELECT
  *,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '${dataset_path}/lab08/ratings',
  format      => 'json',
  schemaHints => 'rating INT'
);

-- Task 2 · SILVER
CREATE OR REFRESH STREAMING TABLE ch08_ratings_silver (
  CONSTRAINT valid_rating EXPECT (rating BETWEEN 1 AND 5)   ON VIOLATION DROP ROW,
  CONSTRAINT has_product  EXPECT (product_id IS NOT NULL)   ON VIOLATION DROP ROW,
  CONSTRAINT has_comment  EXPECT (length(comment) > 0)
)
COMMENT 'Valid ratings (duplicates are removed in gold)'
AS SELECT
  rating_id,
  product_id,
  customer_id,
  rating,
  comment,
  CAST(rated_at AS TIMESTAMP) AS rated_at,
  verified,
  source_file
FROM STREAM(ch08_ratings_bronze);

-- Task 3 · GOLD
CREATE OR REFRESH MATERIALIZED VIEW ch08_product_ratings
COMMENT 'Number of ratings and average rating per known product'
AS SELECT
  p.product_id,
  p.title,
  p.category,
  count(*)                 AS ratings,
  round(avg(r.rating), 2)  AS avg_rating
FROM (SELECT DISTINCT rating_id, product_id, rating FROM ch08_ratings_silver) AS r
JOIN read_files('${dataset_path}/products-csv', format => 'csv', header => true, sep => ';') AS p
  ON r.product_id = p.product_id
GROUP BY p.product_id, p.title, p.category;
