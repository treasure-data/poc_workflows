SELECT table_name, column_name, sample_values
FROM ${project_prefix}_tables_column_metadata
WHERE array_or_json = 1
ORDER BY table_name