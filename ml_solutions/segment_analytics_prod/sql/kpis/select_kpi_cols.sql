SELECT ARRAY_JOIN(ARRAY_AGG(CONCAT('AGG.', column_name)), ', ') as agg_columns
FROM INFORMATION_SCHEMA.columns
WHERE table_schema = '${sink_database}' AND table_name = '${project_prefix}_${table.output_table}' 
AND column_name not in ('time', 'event_date', 'segment_id', 'segment_name')