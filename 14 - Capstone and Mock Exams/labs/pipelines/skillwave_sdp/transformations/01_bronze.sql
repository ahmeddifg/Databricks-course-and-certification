-- =====================================================================================
-- 14-L4 · SkillWave declarative pipeline · BRONZE
-- A STREAMING TABLE reads every enrollment file exactly once (Auto Loader under the hood).
-- ${landing_path} comes from the pipeline Configuration (key landing_path).
-- =====================================================================================
CREATE OR REFRESH STREAMING TABLE sdp_bronze_enrollments
COMMENT 'Raw enrollment events, ingested incrementally from the landing volume'
AS SELECT *,
          _metadata.file_name AS source_file,
          current_timestamp() AS ingested_at
FROM STREAM read_files(
  '${landing_path}/enrollments',
  format => 'json'
);
