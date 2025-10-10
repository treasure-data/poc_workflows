WITH CLEAN AS (
SELECT CAST(
DATE(
COALESCE(TRY(FROM_UNIXTIME(CAST(CAST(${col_name} AS DOUBLE)/1000.0 AS INTEGER))),
COALESCE(TRY(FROM_ISO8601_DATE(rpad(CAST(${col_name} AS VARCHAR), 10, 'o'))),
TRY(date_parse(REPLACE(CAST(${col_name} AS VARCHAR),'/', '-'), '%m-%d-%Y'))
)
)
) 
AS VARCHAR) AS event_date,
count(*) as num_events
FROM ${table_db}.${table_name}
GROUP BY 1
),
T1 as (
SELECT
'${table_db}.${table_name}' as table_name,
'${col_name}' as column_name,
count_if(event_date IS NULL) as null_cnt,
ROUND(CAST(count_if(event_date IS NULL) AS DOUBLE) / SUM(num_events), 3) as null_perc,
approx_distinct(event_date) as distinct_vals,
min(event_date) as oldest_date,
max(event_date) as latest_date,
date_diff('${date_range_format}', min(from_iso8601_timestamp(event_date)), max(from_iso8601_timestamp(event_date))) as time_range_days,
ROUND(avg(num_events), 2) as avg_daily_events
FROM CLEAN
),
T2 as (
SELECT '${table_db}.${table_name}' as table_name,
'${col_name}' as column_name,
event_date,
num_events
FROM CLEAN
ORDER BY num_events desc
LIMIT ${top_k_days_by_event_cnt}
)
SELECT T1.table_name, T1.column_name, T1.null_cnt, T1.null_perc, T1.distinct_vals, T1.oldest_date, T1.latest_date, T1.time_range_days, T1.avg_daily_events, T2.event_date, T2.num_events
FROM T2 JOIN T1
ON T1.table_name = T2.table_name AND T1.column_name = T2.column_name