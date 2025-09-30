WITH METRIC AS (
SELECT 
TD_TIME_STRING(${td.each.unixtime_col}, 'd!') AS event_date,
${td.each.join_key},
'${td.each.activity_name}' AS metric_table,
${td.each.metric_syntax}
FROM ${td.each.src_table}
WHERE TD_INTERVAL(${td.each.unixtime_col}, '${filters.lookback_period}/${filters.time_range_end_date}') AND TD_TIME_STRING(${td.each.unixtime_col}, 'd!') > '${td.last_results.max_date}'
and TD_TIME_STRING(${td.each.unixtime_col}, 'd!') < cast(CURRENT_DATE as varchar)
GROUP BY 1, 2
HAVING ${td.each.having_clause}
)
SELECT 
T1.segment_id,
T1.segment_name,
METRIC.*
FROM ${project_prefix}_run_query T1
JOIN METRIC ON T1.${td.each.join_key} = METRIC.${td.each.join_key}
where T1.segment_id in ${td.last_results.segment_ids}