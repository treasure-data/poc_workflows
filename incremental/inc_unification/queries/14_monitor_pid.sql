
-- Monitoring  persistent_id

 CREATE TABLE  if not exists cdp_unification_${unif_name}.${canonical_id_name}_xref_base as
select
    persistent_id,
    cast (row_number() over () as bigint) seq_id,
    cast(TO_UNIXTIME(current_timestamp) as BIGINT) as create_tm
from
(
  select persistent_id
  from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full
  group by persistent_id
);

drop table if EXISTS cdp_unification_${unif_name}.${canonical_id_name}_xref_base_inc ;
create table cdp_unification_${unif_name}.${canonical_id_name}_xref_base_inc as
with new_profiles as (
  select persistent_id
    from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full
    group by persistent_id
  except
  select persistent_id from cdp_unification_${unif_name}.${canonical_id_name}_xref_base
)
,get_max_seq_from_last_run as
(
  select max(seq_id) latest_cx_id from cdp_unification_${unif_name}.${canonical_id_name}_xref_base
)
select
  persistent_id,
  latest_cx_id + (cast (row_number() over (order by null) as bigint)) seq_id,
  cast(TO_UNIXTIME(current_timestamp) as BIGINT) as create_tm
from new_profiles, get_max_seq_from_last_run;

 insert into  cdp_unification_${unif_name}.${canonical_id_name}_xref_base
 select
  persistent_id,
  seq_id,
  create_tm,
  time
 from cdp_unification_${unif_name}.${canonical_id_name}_xref_base_inc;

---- Monitoring  IDs along with persistent_id

CREATE table if not exists cdp_unification_${unif_name}.${canonical_id_name}_xref_base_key as
select
    k.id key_value
    ,k1.key_name
    ,min(k.id_last_seen_at) key_load_time
    ,k.persistent_id
    ,m.seq_id
from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full  k,
    cdp_unification_${unif_name}.${canonical_id_name}_xref_base m ,
    cdp_unification_${unif_name}.${canonical_id_name}_keys k1
where m.persistent_id = k.persistent_id
      and k.id_key_type = k1.key_type
group by k.persistent_id, m.seq_id, k.id, k1.key_name;

drop table if EXISTS cdp_unification_${unif_name}.${canonical_id_name}_xref_base_key_inc;
CREATE table cdp_unification_${unif_name}.${canonical_id_name}_xref_base_key_inc as
select
    k.id key_value
    ,k1.key_name
    ,min(k.id_last_seen_at) key_load_time,
    k.persistent_id
    ,m.seq_id
from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full  k, -- Picking on inc records
    cdp_unification_${unif_name}.${canonical_id_name}_xref_base m  ,
    cdp_unification_${unif_name}.${canonical_id_name}_keys k1
where m.persistent_id = k.persistent_id
    and k.id_key_type = k1.key_type
group by k.persistent_id, m.seq_id, k.id, k1.key_name;

insert into cdp_unification_${unif_name}.${canonical_id_name}_xref_base_key
with new_key_values as (
select key_value,key_name,persistent_id from cdp_unification_${unif_name}.${canonical_id_name}_xref_base_key_inc
 except
 select  key_value,key_name,persistent_id from  cdp_unification_${unif_name}.${canonical_id_name}_xref_base_key
)
select * from cdp_unification_${unif_name}.${canonical_id_name}_xref_base_key_inc a
where exists (
              select 1 from new_key_values b
              where a.key_value = b.key_value
                AND a.key_name = b.key_name
                and a.persistent_id = b.persistent_id
            )
            ;
