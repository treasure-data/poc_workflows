SELECT
    a.segment_id, 
    a.event_date,
    APPROX_DISTINCT(${td.each.join_key}) AS distinct_profiles,
    '${td.each.activity_name}' AS metric_table,
    '${td.each.metric_name}' AS metric_name,
    ROUND(SUM(TRY(CAST(${td.each.metric_name} AS DOUBLE))), 2) AS metric_value
FROM 
    ${td.each.output_table} a
WHERE 
    a.event_date > (
        SELECT 
            COALESCE(
                MAX(max_event_date), '1950-01-01'
            )
        FROM 
            ${project_prefix}_final_lookup_table 
        WHERE 
            segment_id = a.segment_id 
            AND metric_table = '${td.each.activity_name}_kpis' 
            AND metric_name = '${td.each.metric_name}'
    )
GROUP BY 
    a.segment_id, a.event_date;