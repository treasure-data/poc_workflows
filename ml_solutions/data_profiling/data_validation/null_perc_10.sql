select DISTINCT table_name, column_name, total_rows, null_cnt, null_perc 
FROM reporting.${project_prefix}_numeric_column_stats WHERE null_perc>0.1
UNION ALL
select DISTINCT table_name, column_name,  total_rows, null_cnt, null_perc
FROM reporting.${project_prefix}_varchar_column_stats WHERE null_perc>0.1