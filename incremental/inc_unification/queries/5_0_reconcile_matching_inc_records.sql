-- Insert all inc matched records from earlier unification result into current unification inc table.

drop table if exists ${dest_db}.${unif_src_tbl}_inc_merged;
create table ${dest_db}.${unif_src_tbl}_inc_merged as

select
  ${td.last_results.group_by_cols}, join_key, ingest_time, load_time, time
from ${dest_db}.${unif_src_tbl}_inc

union all

select
  ${td.last_results.group_by_cols},  join_key, ingest_time, load_time, time
 from ${dest_db}.${unif_src_tbl}_inc_matched
;


drop table if exists ${dest_db}.${unif_src_tbl}_inc;
