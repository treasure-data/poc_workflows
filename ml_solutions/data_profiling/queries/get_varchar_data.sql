-- Query below selects VARCHAR columns from the given table we're looping through
select table_name as table_list,
array_agg(column_name) as column_list
 from tables_column_metadata_temp 
where data_type='varchar'
AND table_name = '${table_db}.${table_name}'
AND NOT REGEXP_LIKE(column_name, '${table_exclude_cols}')
group by table_name