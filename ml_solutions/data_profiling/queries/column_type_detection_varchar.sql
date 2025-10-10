SELECT 
'${table_db}.${table_name}' as table_name,
'${col_name}' as column_name,
CASE 
WHEN APPROX_DISTINCT(${col_name}) = 2 THEN 1
ELSE 0 END AS is_bool,
---Added below to detect array/json string columns
CASE
 when cast(count_if(REGEXP_LIKE(${col_name},'^\[.+\]$|^\{.+\}$')=TRUE) as double)/count(*)>${data_threshold} then 1 
 else 0 end as array_or_json,
CASE 
 when cast(count_if(REGEXP_LIKE(${col_name},'^\d{3}-\d{2}-\d{4}$')=TRUE) as double)/count(*)>${data_threshold} then 1 
 else 0 end as is_ssn,
CASE
 when cast(count_if(REGEXP_LIKE(${col_name},'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$')=TRUE) as double)/count(*)>${data_threshold} then 1 
 else 0 end as is_ipaddress, 
CASE
 when cast(count_if(REGEXP_LIKE(${col_name}, '(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))')=TRUE) as double)/count(*)>${data_threshold} then 1 
 else 0 end as is_ipv6_address, 
CASE
 when cast(count_if(REGEXP_LIKE(lower(${col_name}), '\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b')=TRUE) as double)/count(*)>${data_threshold} then 1 
 else 0 end as is_email,
CASE 
 when cast(count_if(REGEXP_LIKE(${col_name}, '^(?:\(?([0-9]{3})\)?[-.●]?)?([0-9]{3})[-.●]?([0-9]{4})$')=TRUE) as double)/count(*)>${data_threshold} then 1 
 when cast(count_if(REGEXP_LIKE(${col_name}, '^(?:\+?1[-.●]?)?\(?([0-9]{3})\)?[-.●]?([0-9]{3})[-.●]?([0-9]{4})$')=TRUE) as double)/count(*)>${data_threshold} then 1 
 when cast(count_if(REGEXP_LIKE(${col_name}, '^\(?([2-9][0-8][0-9])\)?[-.●]?([2-9][0-9]{2})[-.●]?([0-9]{4})$')=TRUE) as double)/count(*)> ${data_threshold} then 1 
else 0 end as is_phone_usa,
CASE 
WHEN cast(count_if((REGEXP_LIKE(${col_name}, '(^[+-]?[0-9]+)|([0-9]*)|([0-9]*)\.([0-9]*)') AND NOT REGEXP_LIKE(${col_name}, '[a-zA-Z]+'))) as double)/count(*)  > ${numeric_flag_threshold} THEN 1
ELSE 0
END as is_num,
CASE
 when cast(count_if(REGEXP_LIKE(${col_name}, '^(?:(?:31(\/|-|\.)(?:0?[13578]|1[02]|(?:Jan|Mar|May|Jul|Aug|Oct|Dec)))\1|(?:(?:29|30)(\/|-|\.)(?:0?[1,3-9]|1[0-2]|(?:Jan|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\2))(?:(?:1[6-9]|[2-9]\d)?\d{2})$|^(?:29(\/|-|\.)(?:0?2|(?:Feb))\3(?:(?:(?:1[6-9]|[2-9]\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?:0?[1-9]|1\d|2[0-8])(\/|-|\.)(?:(?:0?[1-9]|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep))|(?:1[0-2]|(?:Oct|Nov|Dec)))\4(?:(?:1[6-9]|[2-9]\d)?\d{2})$|^(?:[12]\d{3}(\/|-)\d{2}|\d{2}(\/|-)\d{2,4})')=TRUE) as double)/count(*)>${data_threshold} then 1 
 else 0 end as is_date,
 CASE
 when cast(count_if(REGEXP_LIKE(${col_name}, '(^[0-1][0-9]{9}\.?\d{0,3})')=TRUE) as double)/count(*)>${data_threshold} THEN 
(CASE 
WHEN  TRY(TD_TIME_STRING(CAST(rpad(min(${col_name}), 10, '.') AS BIGINT), 'd!')) > '1972-01-01' 
AND TRY(TD_TIME_STRING(CAST(rpad(max(${col_name}), 10, '.') AS BIGINT), 'd!')) < (SELECT CAST(CURRENT_DATE AS VARCHAR))
THEN 1
ELSE 0
END
)
ELSE 0
END AS is_unixtime,
IF(REGEXP_LIKE('${col_name}', '${table_id_or_code_cols}'), 1, 0) AS is_id_or_code

FROM ${table_db}.${table_name} TABLESAMPLE BERNOULLI(${sample_size})
WHERE length(TRIM(${col_name})) > 0
