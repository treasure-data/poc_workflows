SELECT
'${table_db}.${table_name}' as table_name,
'none' as column_name,
CAST(NULL AS BIGINT) as null_cnt,
CAST(NULL AS DOUBLE) as null_perc,
CAST(NULL AS BIGINT) as distinct_vals,
CAST(NULL AS VARCHAR) as oldest_date,
CAST(NULL AS VARCHAR) as latest_date,
CAST(NULL AS BIGINT) AS time_range_days,
CAST(NULL AS DOUBLE) AS avg_daily_events,
CAST(NULL AS VARCHAR) AS event_date,
CAST(NULL AS BIGINT) AS num_events