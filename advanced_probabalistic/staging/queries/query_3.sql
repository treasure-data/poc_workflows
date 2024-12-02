with base as (
  select distinct *    
    --
    , CAST(to_unixtime(date_parse("timestamp", '%Y-%m-%d %H:%i:%s.%f')) AS BIGINT)  as "timestamp_unix"        
  FROM lead2_productofinterests
)
select * 
from base