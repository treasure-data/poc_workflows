DROP TABLE IF EXISTS ${stg}_${sub}.${tbl};
CREATE TABLE ${stg}_${sub}.${tbl} with (
  bucketed_on = array['inc_unix'],
  bucket_count = 512
)  AS
select 
  city
  ,country
  ,state
  ,uuid
  ,REGEXP_REPLACE(
    REPLACE(LOWER(decrypted_encrypted_email), ',', ''),
    '\s',
    ''
  ) AS email
  ,decrypted_encrypted_first_name as first_name
  ,decrypted_encrypted_last_name as last_name
  ,decrypted_encrypted_address1 as address1
  ,decrypted_encrypted_address2 as address2
  ,decrypted_encrypted_zip as zip
  ,decrypted_encrypted_gender as gender
  ,decrypted_encrypted_birthday as birthday
  ,decrypted_encrypted_phone_number as phone_number
  ,time
  ,CONCAT(
    REGEXP_REPLACE(
        LOWER(TRIM(decrypted_encrypted_address1)),
        '[^a-z0-9]',
        ''
    ),
    LOWER(decrypted_encrypted_zip)
) AS composite_address_zip,
TD_TIME_PARSE(CAST(CURRENT_TIMESTAMP as VARCHAR)) as inc_unix
  
from id_conde_rds_decrypted