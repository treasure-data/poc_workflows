WITH VARS AS (
SELECT table_name, column_name, MAX(null_perc) AS null_perc, MAX(distinct_vals) as distinct_vals, ARRAY_AGG(CONCAT(col_value, ': ', TRY(CAST(value_counts AS VARCHAR)))) as top_k_vals
FROM ${project_prefix}_varchar_column_stats
GROUP BY 1, 2
)
,
TMS AS (
  SELECT table_name, column_name, MAX(null_perc) AS null_perc, MAX(oldest_date) as oldest_date, MAX(latest_date) AS latest_date, ROUND(MAX(time_range_days)*1.0 / 365.0, 1) as time_range_years
  FROM ${project_prefix}_date_column_stats
  GROUP BY 1, 2
)
,
COMB AS (
SELECT table_name, column_name, null_perc FROM ${project_prefix}_numeric_column_stats
UNION ALL 
SELECT table_name, column_name, null_perc FROM TMS
UNION ALL 
SELECT table_name, column_name, null_perc FROM VARS
)
SELECT T1.*, T2.sample_values,  COMB.null_perc, VARS.distinct_vals, VARS.top_k_vals, NUM.min_value, NUM.average_value, NUM.max_value, TMS.oldest_date, TMS.latest_date, TMS.time_range_years, T2.tstamp_flag, T2.is_bool, T2.pii_flag
FROM ${project_prefix}_issues_to_check_temp T1
LEFT JOIN ${project_prefix}_tables_column_metadata T2
ON T1.table_name = T2.table_name AND T1.column_name = T2.column_name
LEFT JOIN COMB
ON T1.table_name = COMB.table_name AND T1.column_name = COMB.column_name
LEFT JOIN VARS
ON T1.table_name = VARS.table_name AND T1.column_name = VARS.column_name
LEFT JOIN ${project_prefix}_numeric_column_stats NUM
ON T1.table_name = NUM.table_name AND T1.column_name = NUM.column_name
LEFT JOIN TMS
ON T1.table_name = TMS.table_name AND T1.column_name = TMS.column_name