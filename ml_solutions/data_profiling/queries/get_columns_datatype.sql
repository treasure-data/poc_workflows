--Query extracts metadata about each table from the YML loop from INFORMATION SCHEMA
WITH T1 AS (
select
table_schema || '.' || table_name as table_name
,column_name 
,data_type 
,is_nullable
from information_schema.columns
where table_schema='${table_db}'
and table_name='${table_name}'
AND NOT REGEXP_LIKE(column_name, '${table_exclude_cols}') AND NOT REGEXP_LIKE(data_type, 'array|json')
),
T2 AS (
SELECT '${table_db}.${table_name}' AS table_name, 
COUNT(*) as row_count,
TD_TIME_STRING(MAX(${table_date_col}), 'd!') AS last_updated
FROM ${table_db}.${table_name}
)
SELECT T1.*, T2.row_count, T2.last_updated
FROM T1 JOIN T2 ON
T1.table_name = T2.table_name