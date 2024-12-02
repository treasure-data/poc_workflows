with base as (
  select distinct * 
  --
  , CAST(to_unixtime(date_parse("message_date", '%Y-%m-%d %H:%i:%s.%f')) AS BIGINT) as "message_date_unix"      
  --
  , CAST(to_unixtime(date_parse("cdl_create_date", '%Y-%m-%d %H:%i:%s.%f')) AS BIGINT) as "cdl_create_date_unix" 
  --
  , CAST(to_unixtime(date_parse("cdl_last_update_date", '%Y-%m-%d %H:%i:%s.%f')) AS BIGINT) as "cdl_last_update_date_unix"  
  FROM epsilon_unsubscribes
)
select *
from base