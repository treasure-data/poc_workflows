SELECT DISTINCT column_name, col_pair FROM ${project_prefix}_schema_corr
WHERE table_name = '${table.db}.${table.name}'