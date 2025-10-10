WITH T1 as (
select NUMERIC_HISTOGRAM(${num_bins}, TRY(CAST(${col_name} AS DOUBLE))) as hist
FROM ${table_db}.${table_name}
)
 SELECT '${table_db}.${table_name}' as table_name,
 '${col_name}' as column_name,
 CAST(ROUND(bin_name, 1) as DOUBLE) as bin_label, 
 CAST(num_vals as DOUBLE) as bin_cnt
 
 FROM T1 as x (hist)
 CROSS JOIN UNNEST(hist) as t (bin_name, num_vals)