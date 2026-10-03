-- =====================================================================================
-- 14-L4 · SkillWave declarative pipeline · GOLD
-- A MATERIALIZED VIEW is recomputed (incrementally when possible) on every pipeline update.
-- The DISTINCT removes the duplicate events that the streaming silver table still contains.
-- =====================================================================================
CREATE OR REFRESH MATERIALIZED VIEW sdp_gold_daily_revenue
COMMENT 'Daily enrollments and revenue per category, from the pipeline tables'
AS SELECT to_date(enrolled_at)          AS enrolled_date,
          category,
          count(*)                      AS enrollments,
          round(sum(price_paid), 2)     AS revenue
FROM (SELECT DISTINCT enrollment_id, category, enrolled_at, price_paid FROM sdp_silver_enrollments)
GROUP BY to_date(enrolled_at), category;
