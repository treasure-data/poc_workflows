with table_list as (
  select cast(json_parse('${aggregate_metrics_tables}') AS ARRAY<JSON>) as yml_tbls
),
BASE AS (
SELECT
  json_extract_scalar(yml_tbl, '$.src_table') as src_table,
  json_extract_scalar(yml_tbl, '$.output_table') as output_table,
  json_extract_scalar(yml_tbl, '$.unixtime_col') AS unixtime_col,
  json_extract_scalar(yml_tbl, '$.join_key') AS join_key,
  json_extract_scalar(yml_tbl, '$.apply_time_filter') AS apply_time_filter,
  json_extract_scalar(yml_tbl, '$.table_filter') AS table_filter,
  json_extract_scalar(metrics_parsed, '$.metric_name') AS metric_name,
  json_extract_scalar(metrics_parsed, '$.agg') AS agg_type,
  json_extract_scalar(yml_tbl, '$.query_type') AS query_type,
  json_extract_scalar(metrics_parsed, '$.agg_col_name') AS agg_col_name,
  json_extract_scalar(metrics_parsed, '$.filter') AS filter_rule,
  '${filters.time_filter_type}' AS time_filter_type,
  '${filters.time_range_start_date}'  AS time_range_start_date,
  '${filters.time_range_end_date}'  AS time_range_end_date,
  '${filters.lookback_period}'  AS lookback_period
FROM table_list
CROSS JOIN UNNEST(yml_tbls) AS t(yml_tbl)
CROSS JOIN UNNEST(CAST(json_extract(yml_tbl,'$.metrics')AS ARRAY<JSON>)) AS t(metrics_parsed)
)
,
T1 AS (
SELECT src_table, unixtime_col, join_key, apply_time_filter, output_table, REPLACE(output_table, '_kpis', '') as activity_name, agg_col_name,
metric_name,
CASE 
WHEN filter_rule IS NULL THEN 'ROUND(' || UPPER(agg_type) || '(' || agg_col_name || '), 3) AS ' || metric_name 
ELSE 'ROUND(' || UPPER(agg_type) || '(IF(' || filter_rule || ', ' || agg_col_name || ', NULL)), 3) AS ' || metric_name
END AS metric_syntax,
CASE 
WHEN filter_rule IS NULL THEN UPPER(agg_type) || '(' || agg_col_name || ') > 0'
ELSE UPPER(agg_type) || '(IF(' || filter_rule || ', ' || agg_col_name || ', NULL))  > 0'
END AS having_clause,
'ROUND(SUM(' || metric_name || '), 2) AS ' || metric_name AS column_name,
CASE
WHEN apply_time_filter != 'yes' AND table_filter IS NOT NULL THEN 'AND ' || table_filter
WHEN (apply_time_filter = 'yes' AND time_filter_type = 'range')  AND table_filter IS NULL THEN 'AND TD_TIME_RANGE(' || unixtime_col || ', ''' || time_range_start_date || ''', ''' ||  time_range_end_date || ''' )'
WHEN (apply_time_filter = 'yes' AND time_filter_type = 'range')  AND table_filter IS NOT NULL THEN 'AND TD_TIME_RANGE(' || unixtime_col || ', ''' || time_range_start_date || ''', ''' ||  time_range_end_date || ''' )' || ' AND ' || table_filter
WHEN (apply_time_filter = 'yes' AND time_filter_type = 'interval')  AND table_filter IS NULL THEN 'AND TD_INTERVAL(' || unixtime_col || ', ''' || lookback_period || '/' || 'now' || ''' )'
WHEN (apply_time_filter = 'yes' AND time_filter_type = 'interval')  AND table_filter IS NOT NULL THEN 'AND TD_INTERVAL(' || unixtime_col || ', ''' || lookback_period || '/' || 'now' || ''' )' || ' AND ' || table_filter
ELSE ''
END AS final_where_clause
,query_type 
FROM BASE
)
SELECT src_table,  MAX(unixtime_col) AS unixtime_col, MAX(join_key) AS join_key, MAX(apply_time_filter) AS apply_time_filter, MAX(output_table) AS output_table, MAX(activity_name) AS activity_name, MAX(agg_col_name) AS agg_col_name,
ARRAY_JOIN(ARRAY_AGG(metric_name),', ') as metric_names,
ARRAY_JOIN(ARRAY_AGG(metric_syntax), ', ') as metric_syntax, 
ARRAY_JOIN(ARRAY_AGG(having_clause), ' OR ') as having_clause, 
ARRAY_JOIN(ARRAY_AGG(column_name), ', ') as column_name,
MAX(query_type) as query_type, MAX(final_where_clause) as final_where_clause
FROM T1
GROUP BY 1
