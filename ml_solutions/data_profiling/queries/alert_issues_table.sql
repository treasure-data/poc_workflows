(
SELECT table_name, column_name, data_type,
'data format issue' as issue_type,
CASE 
WHEN data_type = 'varchar' AND is_num = 1 AND tstamp_flag = 0 AND pii_flag = 0 THEN 'Might need to be converted to DOUBLE'
WHEN pii_flag = 1 THEN 'Check if PII should be masked'
ELSE 'none'
END AS issue_to_check
FROM ${project_prefix}_tables_column_metadata
WHERE pii_flag = 1 OR (data_type = 'varchar' AND is_num = 1 AND tstamp_flag =0 AND pii_flag = 0)
)

UNION ALL 

(
SELECT DISTINCT table_name, column_name, 
'TIMESTAMP' AS data_type, 
'tstamp issue' as issue_type,
IF(time_range_days > ${tstamp_days_limit}, 'Check for invalid timestamps', 'none') AS issue_type
FROM ${project_prefix}_date_column_stats
WHERE time_range_days > ${tstamp_days_limit}
)

UNION ALL 

(
SELECT DISTINCT table_name, column_name, 
'NUMERIC' AS data_type, 
'NUMERIC issue' as issue_type,
IF(null_perc > ${null_perc_limit}, 'Too many NULL values', 'none') AS issue_type
FROM ${project_prefix}_numeric_column_stats
WHERE null_perc > ${null_perc_limit}
)

UNION ALL 

(
SELECT DISTINCT table_name, column_name, 
'varchar' AS data_type, 
'VARCHAR issue' as issue_type,
CASE 
WHEN null_perc > ${null_perc_limit} THEN 'Too many NULL values'
WHEN distinct_vals < 2 THEN 'Same value across all rows'
ELSE 'none'
END AS issue_to_check
FROM ${project_prefix}_varchar_column_stats
WHERE distinct_vals < 2 OR null_perc > ${null_perc_limit}
)