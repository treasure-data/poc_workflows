SELECT table_name from INFORMATION_SCHEMA.TABLES
WHERE table_schema = '${sink_database}' and REGEXP_LIKE(table_name, '${project_prefix}_')