SELECT
${session_id} as session_id,
'${column_name.name}' as column_name,
TRY(CAST(${column_name.name} AS VARCHAR)) as col_value,
count(*) as value_counts
FROM ${source_db}.${input_table}
GROUP BY 1, 2, 3
order by 4 desc
limit ${top_k_vals} 