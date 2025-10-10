select DISTINCT table_name, column_name,  null_cnt from ${project_prefix}_date_column_stats where null_cnt>0
UNION ALL
select DISTINCT table_name, column_name, null_cnt from ${project_prefix}_numeric_column_stats where null_cnt>0
UNION ALL
select DISTINCT table_name, column_name, null_cnt from ${project_prefix}_varchar_column_stats where null_cnt>0