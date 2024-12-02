
drop table if exists ${dest_db}.${unif_src_tbl}_inc;

create table ${dest_db}.${unif_src_tbl}_inc as
select
  ${td.last_results.group_by_cols},
  join_key,
  ingest_time,
  load_time,
  time
from ${dest_db}.${dest_tbl}
where load_time > (select coalesce(max(load_time), 0) from cdp_unification_${unif_name}.${unif_src_tbl}_full)
;
