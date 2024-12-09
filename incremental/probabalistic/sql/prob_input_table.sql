with base as  (

  SELECT 
      email
      ,null as composite_address_zip
      ,canonical_id
      ,'enrich_cds_athena_data' as source
  FROM enrich_cds_athena_data

  where email is not null

  UNION ALL

  SELECT 
      email
      ,composite_address_zip
      ,canonical_id
      ,'enrich_id_conde_rds_decrypted' as source
  FROM enrich_id_conde_rds_decrypted

  where email is not null and composite_address_zip is not null

  UNION ALL

  SELECT 
      email
      ,composite_address_zip
      ,canonical_id
      ,'enrich_martech_t_chargebee_subscription_created_decrypted' as source
  FROM enrich_martech_t_chargebee_subscription_created_decrypted

  where email is not null and composite_address_zip is not null

  UNION ALL

  SELECT 
      email
      ,null as composite_address_zip
      ,canonical_id
      ,'enrich_daily_email_updates_decrypted' as source
  FROM enrich_daily_email_updates_decrypted

  where email is not null

)
select distinct email,  coalesce(composite_address_zip, '-999') composite_address_zip, canonical_id
from base
where coalesce(composite_address_zip, '-999') != '-999'
and email not like '%test.com' 
and email not like '%@nonexistant.com' 
and composite_address_zip not like '%1166avenueoftheamericas10036%'