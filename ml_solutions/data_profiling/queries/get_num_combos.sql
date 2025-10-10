WITH T1 as (
-- Query below selects Numerical columns from the given table we're looping through
select table_name,
array_agg(column_name) as column_list
 from ${metadata_table} 
where (data_type='double' OR data_type='bigint' OR is_num = 1)
AND NOT (pii_flag = 1 OR tstamp_flag = 1 OR is_unixtime = 1)
AND table_name = '${table.db}.${table.name}'
AND NOT REGEXP_LIKE(column_name, '${table.exclude_cols}')
group by table_name
),
T2 as (
SELECT table_name, combinations(column_list, 2) as column_combos
FROM T1
)

SELECT table_name, 
pairs[1] as column_name, 
pairs[2] as col_pair
FROM T2
CROSS JOIN UNNEST(column_combos) AS t (pairs)
