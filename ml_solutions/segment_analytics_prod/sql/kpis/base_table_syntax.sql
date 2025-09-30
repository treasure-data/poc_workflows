with table_list as (
  select cast(json_parse('${aggregate_metrics_tables}') AS ARRAY<JSON>) as yml_tbls
)
SELECT
  '${project_prefix}_' || json_extract_scalar(yml_tbl, '$.output_table') as output_table,
  REPLACE(json_extract_scalar(yml_tbl, '$.output_table'), '_kpis', '') as activity_name,
  json_extract_scalar(yml_tbl, '$.join_key') AS join_key,
  json_extract_scalar(metrics_parsed, '$.metric_name') AS metric_name
FROM table_list
CROSS JOIN UNNEST(yml_tbls) AS t(yml_tbl)
CROSS JOIN UNNEST(CAST(json_extract(yml_tbl,'$.metrics')AS ARRAY<JSON>)) AS t(metrics_parsed)
