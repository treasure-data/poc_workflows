SELECT 
'${table_db}.${table_name}' as table_name,
'${col_name}' as column_name,
CASE 
WHEN APPROX_DISTINCT(${col_name}) = 2 THEN 1
ELSE 0 
END AS is_bool,
0 AS array_or_json,
0 AS is_ssn,
0 AS is_ipaddress,
0 AS is_ipv6_address,
0 AS is_email,
---check if a phone number might be present in numeric format
CASE
WHEN cast(count_if(REGEXP_LIKE(TRY(CAST(${col_name} AS VARCHAR)), '^\+?1?\s?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$')=TRUE) as double)/count(*)>0.40 THEN 1
ELSE 0 END AS is_phone_usa, 
1 AS is_num,
0 AS is_date,
CASE
WHEN cast(count_if(REGEXP_LIKE(CAST(CAST(${col_name} AS BIGINT) AS VARCHAR), '(^[0-1][0-9]{9}\.?\d{0,3})')=TRUE) as double)/count(*)>${data_threshold} THEN 
(CASE 
WHEN TRY(TD_TIME_STRING(CAST(min(${col_name}) AS BIGINT), 'd!')) > '1930-01-01' 
AND  TRY(TD_TIME_STRING(CAST(max(${col_name}) AS BIGINT), 'd!')) < CAST(now() AS VARCHAR) 
THEN 1
ELSE 0
END
)
ELSE 0
END AS is_unixtime,
IF(REGEXP_LIKE('${col_name}', '${table_id_or_code_cols}'), 1, 0) AS is_id_or_code

FROM ${table_db}.${table_name} TABLESAMPLE BERNOULLI(${sample_size})
WHERE ${col_name} IS NOT NULL
