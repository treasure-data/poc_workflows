WITH METRIC AS (
SELECT 
TD_TIME_STRING(time, 'd!') AS event_date,
td_canonical_id,
'pageview_events' AS metric_table,
ROUND(COUNT(1), 3) AS pageviews, 
ROUND(COUNT(IF(REGEXP_LIKE(lower(td_url), 'utm_'), 1, NULL)), 3) AS ad_clicks, 
ROUND(COUNT(IF(REGEXP_LIKE(lower(td_url), '=search'), 1, NULL)), 3) AS search_clicks
FROM gldn.enrich_pageviews
WHERE TD_TIME_STRING(time, 'd!') > '${td.last_results.max_date}'
and TD_TIME_STRING(${td.each.unixtime_col}, 'd!') < cast(CURRENT_DATE as varchar)
${td.each.final_where_clause}
GROUP BY 1, 2
HAVING COUNT(1) > 0 OR COUNT(IF(REGEXP_LIKE(lower(td_url), 'utm_'), 1, NULL))  > 0 OR COUNT(IF(REGEXP_LIKE(lower(td_url), '=search'), 1, NULL))  > 0
)
SELECT 
T1.segment_id,
T1.segment_name,
METRIC.*
FROM ${project_prefix}_run_query T1
JOIN METRIC ON T1.td_canonical_id = METRIC.td_canonical_id
where T1.segment_id in ${td.last_results.segment_ids}