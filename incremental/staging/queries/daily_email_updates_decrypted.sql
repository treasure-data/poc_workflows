DROP TABLE IF EXISTS ${stg}_${sub}.${tbl};
CREATE TABLE ${stg}_${sub}.${tbl} with (
  bucketed_on = array['inc_unix'],
  bucket_count = 512
)  AS
select 
  time
  ,REGEXP_REPLACE(
    REPLACE(LOWER(decrypted_encrypted_email), ',', ''),
    '\s',
    ''
  ) AS email
  ,dt
  ,COALESCE(TD_TIME_PARSE(dt), TD_TIME_PARSE(CAST(CURRENT_TIMESTAMP as VARCHAR))) as inc_unix
  ,profile_id



 from daily_email_updates_decrypted