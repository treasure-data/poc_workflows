with max_time as(
select 
  case 
    when (SELECT MAX(time) FROM ${project_prefix}_kpis_combined) > (SELECT max(max_time) FROM ${project_prefix}_final_lookup_table) 
      then (SELECT MAX(time) FROM ${project_prefix}_kpis_combined) 
    else NULL 
  end as max_time
  
),
 KPI AS (
SELECT * FROM ${project_prefix}_kpis_combined
WHERE time = (SELECT max_time FROM max_time)
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

