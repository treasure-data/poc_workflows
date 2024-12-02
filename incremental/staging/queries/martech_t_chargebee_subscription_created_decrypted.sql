DROP TABLE IF EXISTS ${stg}_${sub}.${tbl};
CREATE TABLE ${stg}_${sub}.${tbl} with (
  bucketed_on = array['inc_unix'],
  bucket_count = 512
)  AS
select 
  time
  ,source_legacy_id
  ,user_xid
  ,delta_last_updated_timestamp
  ,email_md5
  ,REGEXP_REPLACE(
    REPLACE(LOWER(decrypted_encrypted_email), ',', ''),
    '\s',
    ''
  ) AS email
  ,decrypted_first_name as first_name
  ,decrypted_last_name as last_name
  ,decrypted_detail_payload_content_customer_billing_address_zip as customer_billing_address_zip
  ,detail_payload_content_customer_billing_address_city as customer_billing_address_city
  ,detail_payload_content_customer_billing_address_country as customer_billing_address_country
  ,decrypted_detail_payload_content_customer_billing_address_line2 as customer_billing_address_line2
  ,decrypted_detail_payload_content_customer_billing_address_line1 as customer_billing_address_line1
  ,CONCAT(
    REGEXP_REPLACE(
        LOWER(TRIM(decrypted_detail_payload_content_customer_billing_address_line1)),
        '[^a-z0-9]',
        ''
    ),
    LOWER(decrypted_detail_payload_content_customer_billing_address_zip)
) AS composite_address_zip,
COALESCE(TD_TIME_PARSE(dt), TD_TIME_PARSE(CAST(CURRENT_TIMESTAMP as VARCHAR))) as inc_unix
 from martech_t_chargebee_subscription_created_decrypted_11_6_2024