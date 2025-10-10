SELECT db_name, table_name, db_name || '.' || table_name as db_table,
RANK() OVER (ORDER BY db_name, table_name) as rank_idx 
FROM ${project_prefix}_db_tables_summary 
WHERE REGEXP_LIKE(table_name, '${full_run_params.tables_to_include}') AND num_columns <= ${full_run_params.col_count_limit}
