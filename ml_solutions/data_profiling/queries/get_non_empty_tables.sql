WITH T1 AS (
  SELECT SPLIT(table_name, '.')[1] AS db_name, SPLIT(table_name, '.')[2] AS table_name, table_name AS  db_table, MAX(row_count) as row_count
  FROM ${metadata_table}_temp
  GROUP BY 1, 2, 3
)
SELECT db_name, table_name, db_table,
RANK() OVER (ORDER BY db_name, table_name) as rank_idx 
FROM T1
WHERE REGEXP_LIKE(table_name, '${full_run_params.tables_to_include}')