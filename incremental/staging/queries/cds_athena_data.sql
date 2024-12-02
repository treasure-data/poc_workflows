DROP TABLE IF EXISTS ${stg}_${sub}.${tbl};
CREATE TABLE ${stg}_${sub}.${tbl} AS
select 
  time
  ,source_legacy_id
  ,email_md5
  ,source_created_dt
  ,source_updated_dt
  ,REGEXP_REPLACE(
    REPLACE(LOWER(decrypted_email), ',', ''),
    '\s',
    ''
  ) AS email,
  CASE 
    WHEN TD_TIME_PARSE(source_updated_dt) < 0 THEN TD_TIME_PARSE(CAST(CURRENT_TIMESTAMP as VARCHAR))
    ELSE COALESCE(TD_TIME_PARSE(source_updated_dt), TD_TIME_PARSE(CAST(CURRENT_TIMESTAMP as VARCHAR))) 
    END as inc_unix
 from cds_athena_data_decrypted