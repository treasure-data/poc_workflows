-- Query below selects date columns from the given table we're looping through
select table_name as table_list,
array_agg(column_name) as column_list
 from ${metadata_table}_temp
WHERE table_name = '${table_db}.${table_name}'
AND (is_date = 1 OR is_unixtime = 1)
group by table_name 