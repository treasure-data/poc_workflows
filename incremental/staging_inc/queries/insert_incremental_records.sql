select 
* from 
  ${tbl}
WHERE TD_TIME_RANGE(inc_unix, 
  ${td.last_results.max_date}, 
  TD_TIME_ADD(${td.last_results.max_date}, '${incremental.time_chunk}')
  )