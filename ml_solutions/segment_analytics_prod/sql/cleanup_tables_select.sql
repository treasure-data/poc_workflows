SELECT table_name FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = '${sink_database}' AND REGEXP_LIKE(table_name, '(?=.*${project_prefix})(?=.*_temp)')  