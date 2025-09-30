WITH METRIC AS (
SELECT 
TD_TIME_STRING(order_datetime_unix, 'd!') AS event_date,
${unique_user_id},
'order_details' AS metric_table,
ROUND(SUM(amount), 2) as total_spend,
ROUND(APPROX_DISTINCT(order_no), 3) AS orders_count,
ROUND(SUM(IF(REGEXP_LIKE(lower(order_transaction_type), 'offline'), amount, NULL)), 3) AS total_spend_offline, 
ROUND(SUM(IF(REGEXP_LIKE(lower(order_transaction_type), 'digital'), amount, NULL)), 3) AS total_spend_digital,
ROUND(SUM(quantity)*1.0, 2) as total_items_ordered
FROM gld_qsr_prod.order_details
WHERE TD_TIME_STRING(${td.each.unixtime_col}, 'd!') > '${td.last_results.max_date}'
AND TD_TIME_STRING(${td.each.unixtime_col}, 'd!') < cast(CURRENT_DATE as varchar)
${td.each.final_where_clause}
GROUP BY 1, 2
-- HAVING SUM(IF(REGEXP_LIKE(lower(order_status), 'complete'), unit_price, NULL))  > 0 OR APPROX_DISTINCT(IF(REGEXP_LIKE(lower(order_status), 'complete'), order_id, NULL))  > 0
)
SELECT 
T1.segment_id,
T1.segment_name,
METRIC.*
FROM ${project_prefix}_run_query T1
JOIN METRIC ON T1.${unique_user_id} = METRIC.${unique_user_id}
WHERE T1.segment_id in ${td.last_results.segment_ids}

