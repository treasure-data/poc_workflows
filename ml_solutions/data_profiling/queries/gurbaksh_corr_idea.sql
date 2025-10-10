SELECT 
'${table.db}.${table.name}' as table_name,
'${td.each.column_name}_${td.each.col_pair}' as col_pair,
ROUND(CAST(corr(CAST(${td.each.column_name} AS DOUBLE), CAST(${td.each.col_pair} AS DOUBLE)) AS DOUBLE), 3) AS pair_corr
FROM ${table.name}