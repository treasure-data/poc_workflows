
---- Creating bkup table of hist to debug if needed after wf is completed.
drop table if exists cdp_unification_${unif_name}.${canonical_id_name}_lookup_full_hist_bkup;
create table cdp_unification_${unif_name}.${canonical_id_name}_lookup_full_hist_bkup
  ${td.last_results.canonical_id_bucket_config}
as
select * from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full_hist
;


---- Creating latest hist table for merge-split users,
drop table if exists cdp_unification_${unif_name}.${canonical_id_name}_lookup_full_hist;
create table cdp_unification_${unif_name}.${canonical_id_name}_lookup_full_hist
  ${td.last_results.canonical_id_bucket_config}
as
select * from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full
;
