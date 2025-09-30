SELECT T1.*, ${td.last_results.agg_columns}
FROM ${project_prefix}_metrics_final_test T1 
LEFT JOIN ${project_prefix}_${table.output_table} AGG 
ON T1.event_date = AGG.event_date AND T1.segment_id = AGG.segment_id