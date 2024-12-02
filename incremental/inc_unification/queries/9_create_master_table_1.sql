drop table if exists cdp_unification_${unif_name}.master_table_tmp;
create table cdp_unification_${unif_name}.master_table_tmp
${td.last_results.canonical_id_bucket_config}
as
select persistent_id from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full
group by persistent_id;
