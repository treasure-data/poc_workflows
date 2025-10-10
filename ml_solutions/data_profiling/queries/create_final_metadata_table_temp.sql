---Parses PII columns and flags all UNIXTIME columns for date conversion later
WITH T3 AS (
SELECT T1.table_name, T1.row_count, T1.column_name, T1.data_type, T1.last_updated, T1.is_nullable, 
COALESCE(T2.is_bool, 0) as is_bool,
COALESCE(T2.array_or_json, 0) as array_or_json,
COALESCE(T2.is_ssn, 0) as is_ssn,
COALESCE(T2.is_ipaddress, 0) AS is_ipaddress,
COALESCE(T2.is_ipv6_address, 0) AS is_ipv6_address,
COALESCE(T2.is_email, 0) AS is_email,
COALESCE(T2.is_phone_usa, 0) AS is_phone_usa,
COALESCE(T2.is_date, 0) AS is_date,
COALESCE(T2.is_num, 1) AS is_num,
COALESCE(T2.is_unixtime, 1) AS is_unixtime,
COALESCE(T2.is_id_or_code, 0) AS is_id_or_code
FROM tables_column_metadata_temp T1
LEFT JOIN tables_column_metadata_datatype_detect T2
ON T1.table_name = T2.table_name AND T1.column_name = T2.column_name
WHERE T1.row_count > ${full_run_params.row_count_limit}
),
T4 AS (
SELECT T3.*,
CASE 
WHEN (is_ssn =1
OR is_ipaddress = 1
OR is_ipv6_address = 1
OR is_email = 1
OR is_phone_usa = 1) THEN 1
ELSE 0
END as pii_flag,
CASE 
WHEN is_unixtime =1 OR is_date = 1 THEN 1
ELSE 0
END as tstamp_flag,
CASE 
WHEN data_type != 'varchar'
THEN 0
ELSE 
(CASE 
WHEN 
is_ssn =1
OR is_ipaddress = 1
OR is_ipv6_address = 1
OR is_email = 1
OR is_phone_usa = 1
OR is_date = 1
OR is_num = 1
OR is_unixtime = 1
THEN 0
ELSE 1
END
) 
END AS categorical_flag
FROM T3
)
SELECT table_name, 
row_count,
column_name, 
data_type,
last_updated, 
pii_flag,
categorical_flag,
tstamp_flag,
array_or_json,
is_bool, 
is_date, 
IF(pii_flag = 0, is_num, 0) AS is_num,
is_unixtime,
is_id_or_code,
is_email, 
is_ssn,
is_ipaddress,
is_ipv6_address,
is_phone_usa,
is_nullable
FROM T4
order by is_num desc