---- ******** Enriched Table Starts

-- -- the inc matched records to be deleted from old enriched table, This table is just for logging purpose and would be deleted at the end of process.
-- drop table if exists cdp_unification_${unif_name}.${unif_src_tbl}_full_purged;
-- create table cdp_unification_${unif_name}.${unif_src_tbl}_full_purged
-- ${td.last_results.bucket_config}
-- -- with (bucketed_on = array['${canonical_id_name}'], bucket_count = 512)
-- as
-- select * from cdp_unification_${unif_name}.${unif_src_tbl}_full
-- where coalesce(${canonical_id_name}, '') in (select ${canonical_id_name} from ${dest_db}.${unif_src_tbl}_inc_matched_id)
-- ;

-- Get all history record from last unification run
drop table if exists cdp_unification_${unif_name}.${unif_src_tbl}_full_hist;
create table cdp_unification_${unif_name}.${unif_src_tbl}_full_hist
${td.last_results.bucket_config}
-- with (bucketed_on = array['${canonical_id_name}'], bucket_count = 512)
as
select * from cdp_unification_${unif_name}.${unif_src_tbl}_full
where coalesce(${canonical_id_name}, '') not in (select ${canonical_id_name} from ${dest_db}.${unif_src_tbl}_inc_matched_id)
;

drop table if exists cdp_unification_${unif_name}.${unif_src_tbl}_full;
alter table cdp_unification_${unif_name}.${unif_src_tbl}_full_hist rename to cdp_unification_${unif_name}.${unif_src_tbl}_full;
---- ******** Enriched Table Ends
