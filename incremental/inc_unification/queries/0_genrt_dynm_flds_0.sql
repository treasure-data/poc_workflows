
with tbl_keys as (
  select
    lower('${merged_keys_list}') as keys_list_raw
)
, tbl_keys_stndrd as (
  select TRANSFORM(split(keys_list_raw, ','), x -> trim(x)) as keys_list,
    array_join(TRANSFORM(split(keys_list_raw, ','), x -> trim(x)), ', ') as keys_list_str,
    array_join(TRANSFORM(split(keys_list_raw, ','), x -> 'b.' || trim(x)), ', ') as keys_list_str_1,
    TRANSFORM(split(keys_list_raw, ','), x -> trim(x)) as keys_for_loop
  from tbl_keys
)

select --keys_list,
  keys_list_str as group_by_cols,
  -- concat('concat(', replace(array_join(TRANSFORM(keys_list, x -> x), ', '), ',' , ', ''${join_key_delimiter}'', '), ')') as join_key_cols, -- This code doesn't handles null and blanks.
  concat('concat(', replace(array_join(TRANSFORM(keys_list, x -> 'coalesce(trim(' || trim(x) || '), '''')'), ', '), ', coalesce(' , ', ''${join_key_delimiter}'', coalesce('), ')') as join_key_cols, -- updated 10/08/2024: This code handles null and blanks.
  concat('coalesce(',array_join(TRANSFORM(keys_list, x -> 'nullif(' || trim(x) || ','''')'), ', '), ') is not null') as where_coalesce_str,
  keys_for_loop,
  keys_list_str_1

  -- Below variables are required for leveraging bucketing on tables.
  , case when '${bucketing_flag}' = 'yes'
    then 'with (bucketed_on = array[''${canonical_id_name}''], bucket_count = 512)'
  else '' end bucket_config,

  case when '${bucketing_flag}' ='yes' and '${run_pid_unif}' = 'yes'
    then 'with (bucketed_on = array[''persistent_id''], bucket_count = 512)'
  when '${bucketing_flag}' ='yes' and '${run_pid_unif}' = 'no'
    then 'with (bucketed_on = array[''canonical_id''], bucket_count = 512)'
  else ''
  end canonical_id_bucket_config,

  exists (select 1 from information_schema.tables where table_schema = 'cdp_unification_${unif_name}' and table_name = '${canonical_id_name}_lookup_full_hist') as table_exists

from tbl_keys_stndrd
