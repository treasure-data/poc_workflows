SELECT
table_schema AS db_name, table_name, COUNT(*) as num_columns
FROM INFORMATION_SCHEMA.COLUMNS
${include_dbs_tbs}
GROUP BY 1, 2