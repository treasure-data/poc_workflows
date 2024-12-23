WITH T1 AS (
  SELECT 
  td_id,
  max(time) as max_unixtime,
  MAX_BY(td_ip, time)  AS td_ip,
  MAX_BY(td_browser, time)  AS td_browser,
  MAX_BY(td_browser_version, time)  AS td_browser_version,
  MAX_BY(td_os, time)  AS td_os,
  MAX_BY(td_viewport, time)  AS td_viewport
  FROM ${base_table}
  GROUP BY 1
)
SELECT T1.*,
CONCAT(TRY(REGEXP_REPLACE(lower(td_ip), '\.| ', '_')), 
       TRY(REGEXP_REPLACE(lower(td_browser), '\.| ', '_')), 
       TRY(REGEXP_REPLACE(lower(td_browser_version), '\.| ', '_')), 
       TRY(REGEXP_REPLACE(lower(td_os), '\.| ', '_')), 
       TRY(REGEXP_REPLACE(lower(td_viewport), '\.| ', '_'))
       ) as device_id_concat,
date_diff('day', FROM_UNIXTIME(max_unixtime), NOW()) as days_since_last_activity
FROM T1