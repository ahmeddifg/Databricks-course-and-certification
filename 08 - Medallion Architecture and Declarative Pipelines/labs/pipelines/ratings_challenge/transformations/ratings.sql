-- =====================================================================================
-- 08-L4 · CHALLENGE · ShopWave product ratings pipeline
-- Write your pipeline code in THIS file (open it from the pipeline editor).
-- Rules: no hard-coded /Volumes paths - use ${dataset_path} (pipeline Configuration).
--        Table names must match exactly - the checks in the 08-L4 notebook look for them.
-- The ratings land in:  ${dataset_path}/lab08/ratings   (JSON lines)
-- Columns: rating_id, product_id, customer_id, rating, comment, rated_at, verified
--          (+ helpful_votes from file ratings_03.json on)
-- Products: ${dataset_path}/products-csv  (CSV, header, ';' delimited)
-- =====================================================================================

-- TODO Task 1 · BRONZE streaming table  ch08_ratings_bronze
--   * incremental ingestion of the ratings landing folder (JSON) - hint: STREAM read_files(...)
--   * all source columns + a column  source_file  (the file name)
--   * make sure  rating  is an INT (schemaHints)



-- TODO Task 2 · SILVER streaming table  ch08_ratings_silver   (reads bronze as a stream)
--   * expectation  valid_rating : rating between 1 and 5        -> DROP the row
--   * expectation  has_product  : product_id is not null        -> DROP the row
--   * expectation  has_comment  : comment is not empty          -> WARN only (keep the row)
--   * columns: rating_id, product_id, customer_id, rating, comment,
--              rated_at (as TIMESTAMP), verified, source_file



-- TODO Task 3 · GOLD materialized view  ch08_product_ratings
--   * one row per KNOWN product (inner join with the products CSV)
--   * columns: product_id, title, category, ratings (number of DISTINCT rating_id), avg_rating (rounded to 2 decimals)
--   * the app re-sends some ratings: count and average every rating_id only once

