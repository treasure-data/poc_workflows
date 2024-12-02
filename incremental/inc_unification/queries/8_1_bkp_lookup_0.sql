
drop table if exists cdp_unification_${unif_name}.${canonical_id_name}_lookup_full;
create table cdp_unification_${unif_name}.${canonical_id_name}_lookup_full
  ${td.last_results.canonical_id_bucket_config}
as
select * from cdp_unification_${unif_name}.${canonical_id_name}_lookup
;
