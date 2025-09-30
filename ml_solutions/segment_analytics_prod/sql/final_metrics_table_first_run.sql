SELECT
T1.*,
0 as query_change,
CAST(time as DOUBLE) as date_unixtime,
TD_TIME_STRING(time, 's!') as run_time
FROM ${project_prefix}_final_metrics_temp T1