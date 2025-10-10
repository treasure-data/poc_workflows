WITH T1 as (
SELECT
'${table_db}.${table_name}' as table_name,
'${col_name}' as column_name,
min(TD_TIME_STRING(CAST(CAST(${col_name} AS DOUBLE) AS INTEGER), 'd!')) as oldest_date,
max(TD_TIME_STRING(CAST(CAST(${col_name} AS DOUBLE) AS INTEGER), 'd!')) as latest_date,
date_diff('day', min(from_unixtime(CAST(CAST(${col_name} AS DOUBLE) AS INTEGER))), max(from_unixtime(CAST(CAST(${col_name} AS DOUBLE) AS INTEGER)))) time_range_days
FROM dilyan_demo.${project_prefix}_attributes
),
T2 as (
SELECT '${table_db}.${table_name}' as table_name,
'${col_name}' as column_name,
TD_TIME_STRING(CAST(CAST(${col_name} AS DOUBLE) AS INTEGER), 'd!') as event_date,
(SELECT avg(events) FROM 
(select TD_TIME_STRING(CAST(CAST(${col_name} AS DOUBLE) AS INTEGER), 'd!') as event_date, count(*) as events FROM ${project_prefix}_attributes GROUP BY 1)) as avg_daily_events,
count(*) as num_events
FROM dilyan_demo.${project_prefix}_attributes
GROUP BY 1, 2, 3, 4
ORDER BY 5 desc
LIMIT 20
)
SELECT T2.table_name, T2.column_name, T1.oldest_date, T1.latest_date, T1.time_range_days, T2.event_date, T2.num_events, T2.avg_events
FROM T2 JOIN T1
ON T1.table_name = T2.table_name AND T1.column_name = T2.column_name