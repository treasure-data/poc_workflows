SELECT 
'${table.db}.${table.name}' as table_name,
'${td.each.column_name}' as column_name,
'${td.each.col_pair}' as corr_col,
'${td.each.column_name}_${td.each.col_pair}' as col_pair,
ROUND(CAST(corr(TRY(CAST(${td.each.column_name} AS DOUBLE)), TRY(CAST(${td.each.col_pair} AS DOUBLE))) AS DOUBLE), 3) AS pair_corr
FROM ${table.db}.${table.name}