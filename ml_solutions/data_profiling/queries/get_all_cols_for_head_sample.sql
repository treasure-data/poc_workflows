-- Query below selects ALL columns so we can get a head sample
SELECT table_name AS table_list,
array_agg(column_name) AS column_list
FROM ${metadata_table}_temp
WHERE table_name = '${table_db}.${table_name}'
AND column_name IN (SELECT column_name FROM ${metadata_table}_temp WHERE table_name = '${table_db}.${table_name}' ${varchar_params.exclude_pii})
GROUP BY table_name