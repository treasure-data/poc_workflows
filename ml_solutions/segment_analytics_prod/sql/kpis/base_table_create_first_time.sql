SELECT
a.segment_id, a.event_date,
APPROX_DISTINCT(${td.each.join_key}) as distinct_profiles,
'${td.each.activity_name}' AS metric_table,
'${td.each.metric_name}' AS metric_name,
ROUND(SUM(TRY(CAST(${td.each.metric_name} AS DOUBLE))), 2) AS metric_value
FROM ${td.each.output_table} a
where event_date > '1950-01-01'
GROUP BY 1, 2