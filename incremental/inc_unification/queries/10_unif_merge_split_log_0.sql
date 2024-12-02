
---------------------------------------*********************-------------------------------------------

create table if not exists cdp_unification_${unif_name}.unif_merge_split_log (time bigint);

Insert Into cdp_unification_${unif_name}.unif_merge_split_log
with prev as (
  select
    l.id,
    k.key_name,
    l.canonical_id as historical_canonical_id
  from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full_hist l-- Yesterday's or previous unfication lookup table
  inner join cdp_unification_${unif_name}.${canonical_id_name}_keys k on k.key_type = l.id_key_type
  where nullif(canonical_id, '') is not null
),
curr as (
  select
    l.id,
    k.key_name,
    l.canonical_id as current_canonical_id
  from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full l -- Today's unfication lookup table
  inner join cdp_unification_${unif_name}.${canonical_id_name}_keys k on k.key_type = l.id_key_type
  where nullif(canonical_id, '') is not null
),
combine as (
  select
    COALESCE(a.id, b.id) as id,
    COALESCE(a.key_name, b.key_name) as key_name,
    COALESCE(b.historical_canonical_id, a.current_canonical_id) as historical_canonical_id,
    COALESCE(a.current_canonical_id, b.historical_canonical_id) as current_canonical_id
  from curr a
  full outer join prev b on a.id = b.id and a.key_name = b.key_name
),
combine_selected as (
  select distinct
    historical_canonical_id,
    current_canonical_id
  from combine
)

select
  'MERGE' as action,
  ARRAY_AGG(historical_canonical_id) as historical_canonical_id_array,
  current_canonical_id,
  cast(null as ARRAY<varchar>) as current_canonical_id_array,
  cast(null as varchar) as historical_canonical_id,
  ${session_id} as session_id
from combine_selected
group by current_canonical_id
having count(distinct historical_canonical_id) > 1

UNION ALL

select
  'SPLIT' as action,
  cast(null as ARRAY<varchar>) as historical_canonical_id_array,
  cast(null as varchar) as current_canonical_id,
  ARRAY_AGG(current_canonical_id) as current_canonical_id_array,
  historical_canonical_id,
  ${session_id} as session_id
from combine_selected
group by historical_canonical_id
having count(distinct current_canonical_id) > 1

---------------------------------------*********************-------------------------------------------
