WITH KPI AS (
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
FROM KPI
JOIN ${project_prefix}_segment_profile_mapping BASE
ON KPI.segment_id = BASE.segment_id
