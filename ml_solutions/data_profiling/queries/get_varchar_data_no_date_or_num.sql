-- Query below selects VARCHAR columns from the given table and excludes numbers and dates as VARCHAR
SELECT table_name as table_list,
array_agg(column_name) as column_list
FROM ${metadata_table}_temp
WHERE table_name = '${table_db}.${table_name}'
AND ((data_type='varchar' AND NOT REGEXP_LIKE(column_name, '${table_exclude_cols}') AND is_num = 0  AND is_date = 0 ${varchar_params.exclude_pii} ${varchar_params.exclude_array_or_json}) 
  OR is_id_or_code = 1)
group by table_name