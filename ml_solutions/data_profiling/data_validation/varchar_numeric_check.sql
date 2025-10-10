SELECT * FROM ${sink_database}.${project_prefix}_tables_column_metadata
WHERE data_type = 'varchar' and is_num = 1