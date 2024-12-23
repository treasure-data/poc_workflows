SELECT
table_name
FROM information_schema.tables
where table_schema='${sink_database}'
AND REGEXP_LIKE(table_name, 'prll_25m|prl_5m') 