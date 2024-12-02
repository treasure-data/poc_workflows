create table if not exists ${stg}_${sub}_inc.${tbl} as 

select 

    0 as stg_inc_record_count, 
    min(inc_unix) as inc_unix 

from ${tbl} 