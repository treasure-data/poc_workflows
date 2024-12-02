
drop table if exists cdp_unification_${unif_name}.${unif_src_tbl}_full;
create table cdp_unification_${unif_name}.${unif_src_tbl}_full
${td.last_results.bucket_config}
as
select * from cdp_unification_${unif_name}.enriched_${unif_src_tbl}
;
