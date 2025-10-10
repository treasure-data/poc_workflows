SELECT ${td.last_results.column_name} as ${td.last_results.column_name}, count(*) as cent
FROM ${td.last_results.table_name}
GROUP BY 1