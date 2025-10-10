-- Query below selects Numerical columns from the given table we're looping through
select table_name as table_list,
array_agg(column_name) as column_list
 from ${metadata_table}_temp
where tstamp_flag = 0 AND (data_type='double' OR data_type='bigint' OR is_num = 1)
AND NOT (pii_flag = 1 OR tstamp_flag = 1 OR is_unixtime = 1 OR is_id_or_code = 1)
AND table_name = '${table_db}.${table_name}'
AND NOT REGEXP_LIKE(column_name, '${table_exclude_cols}')
group by table_name 