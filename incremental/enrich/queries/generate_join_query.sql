with config as (select json_parse('${tables}') as raw_details),

tbl_config as (
select
  cast(json_extract(tbl_details,'$.database') as varchar) as database,
  json_extract(tbl_details,'$.key_columns') as key_columns,
  cast(json_extract(tbl_details,'$.table') as varchar) as tbl

from
(
select tbl_details
FROM config
CROSS JOIN UNNEST(cast(raw_details as ARRAY<JSON>)) AS t (tbl_details))),

column_config as (select
  database,
  tbl,
  cast(json_extract(key_column,'$.column') as varchar) as table_field,
  cast(json_extract(key_column,'$.key') as varchar) as unification_key
from
  tbl_config
CROSS JOIN UNNEST(cast(key_columns as ARRAY<JSON>)) AS t (key_column)),

final_config as (
select
  tc.*,
  k.key_type
from
column_config tc
left join
(select distinct key_type, key_name from ${canonical_id}_keys) k
on tc.unification_key = k.key_name),

join_config as (select
database,
tbl,
table_field,
unification_key,
key_type,
-- case when engine = 'presto' then
'when cast(nullif(CAST(p.' || table_field || ' AS VARCHAR), '''') as varchar) is not null then cast(p.' || table_field || ' as varchar)'
-- else
-- 'when cast(nullif(p.' || table_field || ', '''') as string) is not null then cast(p.' || table_field || ' as string)'
-- end 
as id_case_sub_query,
-- case when engine = 'presto' then
'when cast(nullif(CAST(p.' || table_field || ' AS VARCHAR), '''') as varchar) is not null then ' || coalesce(cast(key_type as varchar),'no key')
-- else
-- 'when cast(nullif(p.' || table_field || ', '''') as string) is not null then ' || coalesce(cast(key_type as varchar),'no key')
-- end 
as key_case_sub_query
from final_config),

join_conditions as (select
  database,
  tbl,
  -- case when engine = 'presto' then
  'left join cdp_unification_${sub}.${canonical_id}_lookup_full k0' || chr(10) || '  on k0.id = case ' || array_join(array_agg(id_case_sub_query),chr(10)) || chr(10) || 'else null end'
  -- else
  -- 'left join cdp_unification_${sub}.${canonical_id}_lookup_full k0' || chr(10) || '  on k0.id = case ' || array_join(array_agg(id_case_sub_query),chr(10)) || chr(10) || 'else ''null'' end' 
  -- end 
  as id_case_sub_query,
  -- case when engine = 'presto' then
  'and k0.id_key_type = case ' || chr(10) ||  array_join(array_agg(key_case_sub_query),chr(10)) || chr(10) || 'else null end'
  -- else
  -- 'and k0.id_key_type = case ' || chr(10) ||  array_join(array_agg(key_case_sub_query),chr(10)) || chr(10) || 'else 0 end'
  -- end 
  as key_case_sub_query
from
  join_config
group by
  database, tbl),

field_config as (SELECT
  table_schema as database,
  table_name as tbl,
  array_join(array_agg(column_name), CONCAT (',',chr(10))) AS fields
FROM (
	  SELECT table_schema, table_name, concat('p.' , column_name) column_name
	    FROM information_schema.COLUMNS
      where column_name not in (select distinct table_field from final_config)
    union
    SELECT table_schema, table_name,
    concat('nullif(CAST(p.', column_name, ' as VARCHAR),', '''''' ,') as ', column_name)  column_name
	  FROM information_schema.COLUMNS
    where column_name in (select distinct table_field from final_config)
	) x
group by table_schema,table_name),

query_config as (select
  j.database,
  j.tbl,
  id_case_sub_query || chr(10) || key_case_sub_query as join_sub_query,
  f.fields
from
  join_conditions j
left join
  field_config f
on j.database = f.database
and j.tbl = f.tbl)

select
  'select ' || chr(10) ||
    fields || ',' || chr(10) ||
    'k0.persistent_id as ' || '${canonical_id}' || chr(10) ||
  'from ' || chr(10) ||
    database || '.' || tbl ||' p' || chr(10) ||
  join_sub_query as query,
  tbl as tbl
from
  query_config
  order by tbl desc
