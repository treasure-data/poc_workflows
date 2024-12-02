DROP TABLE IF EXISTS ${stg}_${sub}.${tbl};
CREATE TABLE ${stg}_${sub}.${tbl} with (
  bucketed_on = array['inc_unix'],
  bucket_count = 512
)  AS
select *, 
COALESCE(TD_TIME_PARSE(insert_tsp), TD_TIME_PARSE(CAST(CURRENT_TIMESTAMP as VARCHAR))) as inc_unix
from martech_d_email_extd