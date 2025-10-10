SELECT
'${table_db}.${table_name}' as table_name,
'none' as column_name,
CAST(NULL AS BIGINT) as total_rows,
CAST(NULL AS BIGINT) as null_cnt,
CAST(NULL AS DOUBLE) as null_perc,
CAST(NULL AS BIGINT)  as distinct_vals,
CAST(NULL AS VARCHAR) as col_value,
CAST(NULL AS BIGINT) as value_counts