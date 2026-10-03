-- =====================================================================================
-- 14-L4 · SkillWave declarative pipeline · SILVER
-- Expectations = data quality rules with an action on violation:
--   ON VIOLATION FAIL UPDATE -> stop the update      ON VIOLATION DROP ROW -> drop the row
--   (no ON VIOLATION)        -> keep the row, only count it in the metrics (warn)
-- The join to silver.courses (a regular Delta table) is a stream-static join.
-- =====================================================================================
CREATE OR REFRESH STREAMING TABLE sdp_silver_enrollments (
  CONSTRAINT valid_id     EXPECT (enrollment_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_price  EXPECT (price_paid >= 0)           ON VIOLATION DROP ROW,
  CONSTRAINT known_course EXPECT (course_title IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT known_coupon EXPECT (coupon IS NULL OR coupon IN ('WELCOME10', 'SUMMER20', 'VIP50'))
)
COMMENT 'Validated enrollments enriched with the course title and category'
AS SELECT e.enrollment_id,
          e.student_id,
          e.course_id,
          c.title                              AS course_title,
          c.category,
          CAST(e.enrolled_at AS TIMESTAMP)     AS enrolled_at,
          CAST(e.price_paid AS DECIMAL(10,2))  AS price_paid,
          e.coupon,
          e.channel,
          e.status,
          e.source_file
FROM STREAM(sdp_bronze_enrollments) e
LEFT JOIN silver.courses c ON e.course_id = c.course_id;
