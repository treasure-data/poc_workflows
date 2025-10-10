---Query below gets a sample of each column in the table for the HEAD5 WIdget
WITH T1 AS (
SELECT '${table_db}.${table_name}' as table_name,
'${col_name}'  as column_name,
CASE 
    WHEN typeof(${col_name}) = 'double' THEN format('%.2f', ${col_name}) -- Format DOUBLE values as regular decimals
    ELSE CAST(${col_name} AS VARCHAR) -- Cast other types to VARCHAR
END AS ${col_name}
FROM ${table_db}.${table_name}
WHERE ${col_name} IS NOT NULL AND NOT REGEXP_LIKE(TRY(CAST(${col_name} AS VARCHAR)), '^[ ]*$')
LIMIT ${head_sample}
)
SELECT table_name,
column_name,
array_join(array_agg(${col_name}), ' | ') as sample_values
FROM T1
GROUP by 1, 2