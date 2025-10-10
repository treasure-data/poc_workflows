SELECT 
T1.table_name,
SPLIT(T1.table_name, '.')[1] AS db_name, 
SPLIT(T1.table_name, '.')[2] AS tab_name,
T1.row_count, 
T1.column_name, 
T1.data_type,
HEAD.sample_values,
IF(T1.last_updated < '2000-01-01', CAST(CURRENT_DATE AS VARCHAR), T1.last_updated) AS last_updated,
IF(T1.tstamp_flag = 1, 0, T1.pii_flag) AS pii_flag,
T1.categorical_flag,
T1.tstamp_flag,
T1.is_bool,
T1.array_or_json, 
T1.is_date, 
T1.is_num,
T1.is_unixtime,
T1.is_email, 
T1.is_ssn,
T1.is_ipaddress,
T1.is_ipv6_address,
IF(T1.tstamp_flag = 1, 0, T1.is_phone_usa) AS is_phone_usa,
T1.is_nullable,
T1.is_id_or_code
FROM ${metadata_table}_temp T1
LEFT JOIN tables_column_metadata_head HEAD
ON T1.table_name = HEAD.table_name AND T1.column_name = HEAD.column_name