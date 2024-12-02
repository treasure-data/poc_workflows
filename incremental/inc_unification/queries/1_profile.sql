
create table if not exists ${dest_db}.${dest_tbl} as
select
  ${td.last_results.group_by_cols},
  ${td.last_results.join_key_cols} as join_key,
  min(ingest_time) as ingest_time,
  TD_TIME_PARSE(cast(CURRENT_TIMESTAMP as varchar)) as load_time,
  min(time) as time
from ${src_db}.${src_tbl}
where ${td.last_results.where_coalesce_str}
group by ${td.last_results.group_by_cols}
;


-- -- Appending Only INC data to Work table.
insert into ${dest_db}.${dest_tbl}
with get_inc_data as
(
  select
    ${td.last_results.group_by_cols},
    ${td.last_results.join_key_cols} as join_key,
    min(ingest_time) as ingest_time,
    min(time) as time
  from ${src_db}.${src_tbl}
  where ingest_time > (select coalesce(max(ingest_time), 0) as ingest_time from ${dest_db}.${dest_tbl})
    and ${td.last_results.where_coalesce_str}
  group by ${td.last_results.group_by_cols}
)
select
  *
  , TD_TIME_PARSE(cast(CURRENT_TIMESTAMP as varchar)) as load_time -- This is to make sure that all inc data gets picked up evenif custom_inc_unif did not run or fails.
from get_inc_data a
where not exists (select 1 from ${dest_db}.${dest_tbl} b where a.join_key = b.join_key)
;
