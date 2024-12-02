---- ******** Lookup Table Starts

-- -- Get all matching users from pervious ${canonical_id_name}_lookup table and store into cdp_unification_${unif_name}.${canonical_id_name}_lookup_full_purged.
-- -- the inc matched records to be deleted from old lookup table, This table is just for logging purpose and would be deleted at the end of process.
-- drop table if exists cdp_unification_${unif_name}.${canonical_id_name}_lookup_full_purged;
-- create table cdp_unification_${unif_name}.${canonical_id_name}_lookup_full_purged
-- ${td.last_results.canonical_id_bucket_config}
-- -- with (bucketed_on = array['persistent_id'], bucket_count = 512)
-- as
-- select * from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full
-- where coalesce(persistent_id, '') in (select ${canonical_id_name} from ${dest_db}.${unif_src_tbl}_inc_matched_id)
-- ;


-- Get all non-matching users from pervious ${canonical_id_name}_lookup table and store into cdp_unification_${unif_name}.${canonical_id_name}_lookup_${session_unixtime}_hist.
-- This table would be merged with latest unfication result.
drop table if exists cdp_unification_${unif_name}.${canonical_id_name}_lookup_${session_unixtime}_hist;
create table cdp_unification_${unif_name}.${canonical_id_name}_lookup_${session_unixtime}_hist
${td.last_results.canonical_id_bucket_config}
-- with (bucketed_on = array['persistent_id'], bucket_count = 512)
as
select * from cdp_unification_${unif_name}.${canonical_id_name}_lookup_full
where coalesce(persistent_id, '') not in (select ${canonical_id_name} from ${dest_db}.${unif_src_tbl}_inc_matched_id)
;

drop table if exists cdp_unification_${unif_name}.${canonical_id_name}_lookup_full;
alter table cdp_unification_${unif_name}.${canonical_id_name}_lookup_${session_unixtime}_hist rename to cdp_unification_${unif_name}.${canonical_id_name}_lookup_full;
---- ******** Lookup Table Ends
