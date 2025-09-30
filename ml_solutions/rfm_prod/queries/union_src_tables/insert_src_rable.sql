SELECT ${td.each.join_key} AS ${globals.canonical_id},
MAX(${td.each.unixtime_col}) as time,
TD_TIME_STRING(MAX(${td.each.unixtime_col}), 's!') as latest_tstamp,
'${td.each.event_name}' AS source,
COUNT(*) as total_touchpoints,
SUM(TRY(CAST(${td.each.order_amount} AS DOUBLE))) as total_spend,
(${session_unixtime} - MAX(${td.each.unixtime_col}))/86400 AS recency_days
FROM ${td.each.src_table}
${td.each.final_where_clause}
GROUP BY 1
