
select
 '${table_name}' as  tb 
 ,'${column}' as  column
 ,(select COUNT_IF(${column} is NULL) from ${table_name}) as null_cnt
,(select approx_distinct(${column}) from ${table_name}) as aprx_distinct_cnt 
,(select MIN(LENGTH(${column})) from  ${table_name}) as min_length

 from ${table_name} limit 1