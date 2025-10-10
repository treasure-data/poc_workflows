-- Query below gets aggregate stats on total rows, null count, distinct value count
WITH T1 as (
select
'${table_db}.${table_name}' as table_name,
'${col_name}' as column_name,
count(*) as total_rows,
count_if(${col_name} IS NULL) as null_cnt,
ROUND(CAST(count_if(${col_name} IS NULL) AS DOUBLE) / count(*), 3) as null_perc,
APPROX_DISTINCT(${col_name}) as distinct_vals
from ${table_db}.${table_name}
GROUP BY 1, 2
HAVING count(*) > ${varchar_params.min_rows}
),
-- Query below gets the value counts for each unique value in column for the top_k vals by count controled by the top_k_vals param in YML and JOINS to T1 on table_name
T2 as (
SELECT * FROM (
select
'${table_db}.${table_name}' as table_name,
'${col_name}' as column_name,
TRY(CAST(${col_name} AS VARCHAR)) as col_value,
count(*) as value_counts
FROM ${table_db}.${table_name}
GROUP BY 1, 2, 3
order by value_counts desc
limit ${varchar_params.top_k_vals}
)
)
Select T1.*, T2.col_value, T2.value_counts
FROM T1 JOIN T2 
ON T1.table_name = T2.table_name