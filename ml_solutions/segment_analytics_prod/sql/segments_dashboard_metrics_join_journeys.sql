WITH T2 AS (
  SELECT DISTINCT segment_id, journey_name FROM segment_analytics_journey_stats
  WHERE time = (SELECT MAX(time) FROM segment_analytics_journey_stats)
)
,
BASE AS (
select T1.*,
T2.journey_name
FROM ${project_prefix}_segment_profile_mapping T1
LEFT JOIN T2
ON T1.segment_id = T2.segment_id
)
,
KPI AS (
SELECT * FROM ${project_prefix}_kpis_combined
WHERE time = (SELECT MAX(time) FROM ${project_prefix}_kpis_combined)
)
SELECT
BASE.*,
KPI.event_date,
KPI.distinct_profiles,
KPI.metric_table,
KPI.metric_name,
KPI.metric_value
FROM KPI JOIN BASE
ON KPI.segment_id = BASE.segment_id