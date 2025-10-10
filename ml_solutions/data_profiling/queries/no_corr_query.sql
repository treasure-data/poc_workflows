SELECT
'${table.db}.${table.name}' as table_name,
'none' as column_name,
CAST(NULL AS VARCHAR) as corr_col,
CAST(NULL AS VARCHAR) as col_pair,
CAST(NULL AS DOUBLE) as pair_corr
