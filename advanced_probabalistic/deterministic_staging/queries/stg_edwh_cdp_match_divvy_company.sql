with base as (
  SELECT
    divvy_uuid,
    duns_number,
    lower(company_name) as company_name,
    lower(company_email_domain) as company_email_domain,
    address_line_1,
    address_line_2,
    case
        when nullif(lower(trim("address_line_1")), 'null') is null then null
        when nullif(lower(trim("address_line_1")), '') is null then null
        else array_join((transform((split(lower(trim("address_line_1")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_address1",
    case
        when nullif(lower(trim("address_line_2")), 'null') is null then null
        when nullif(lower(trim("address_line_2")), '') is null then null
        else array_join((transform((split(lower(trim("address_line_2")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_address2",
case
        when nullif(lower(trim("postal_code")), 'null') is null then null
        when nullif(lower(trim("postal_code")), '') is null then null
        else array_join((transform((split(lower(trim("postal_code")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
              end   AS  "trfmd_addresszip",
    city,
    state,
    postal_code,
    country,
    mailing_address_line_1,
    mailing_address_line_2,
    mailing_city,
    mailing_state,
    mailing_postal_code,
    mailing_country,
    tax_id_hash,
    bank_routing_number,
    bank_account_number_hash,
    time
FROM
    bill_source.edwh_cdp_match_divvy_company
), 

prob_company_name as (
SELECT 
  company_name, 
  avg_cluster_similarity, 
  CASE 
  WHEN avg_cluster_similarity >= .95 THEN cluster_id
    ELSE null
  END AS company_name_95_id,
  CASE 
  WHEN avg_cluster_similarity >= .90 THEN cluster_id
    ELSE null
  END AS company_name_90_id,
  CASE 
  WHEN avg_cluster_similarity >= .80 THEN cluster_id
    ELSE null
  END AS company_name_80_id, 
  CASE 
  WHEN avg_cluster_similarity >= .70 THEN cluster_id
    ELSE null
  END AS company_name_70_id,
  CASE 
  WHEN avg_cluster_similarity >= .65 THEN cluster_id
    ELSE null
  END AS company_name_65_id
  FROM 
  cdp_unification_probabalistic_advanced_bill_divvy.prob_final_cluster_company_name_80
),


prob_address as (
SELECT 
  distinct
  address, 
  avg_cluster_similarity, 
  CASE 
  WHEN avg_cluster_similarity >= .95 THEN cluster_id
    ELSE null
  END AS address_95_id,
  CASE 
  WHEN avg_cluster_similarity >= .90 THEN cluster_id
    ELSE null
  END AS address_90_id,
  CASE 
  WHEN avg_cluster_similarity >= .80 THEN cluster_id
    ELSE null
  END AS address_80_id, 
  CASE 
  WHEN avg_cluster_similarity >= .70 THEN cluster_id
    ELSE null
  END AS address_70_id,
  CASE 
  WHEN avg_cluster_similarity >= .65 THEN cluster_id
    ELSE null
  END AS address_65_id
  FROM 
    cdp_unification_probabalistic_advanced_bill_divvy.prob_final_cluster_address_80 WHERE address in (
    select address from cdp_unification_probabalistic_advanced_bill_divvy.prob_final_cluster_address_80 group by 1 having count(distinct cluster_id) = 1
  )
)

SELECT b.*, 
p_cn.company_name_95_id, 
p_cn.company_name_90_id, 
p_cn.company_name_80_id, 
p_cn.company_name_70_id, 
p_cn.company_name_65_id,
p_ad.address_95_id, 
p_ad.address_90_id, 
p_ad.address_80_id, 
p_ad.address_70_id, 
p_ad.address_65_id,
-- rule #2
concat(tax_id_hash, bank_account_number_hash, bank_routing_number, p_ad.address_70_id, trfmd_addresszip, p_cn.company_name_70_id) as tax_accnt_routing_addr70_zip_name70,
-- rule #3
concat(tax_id_hash, bank_account_number_hash, bank_routing_number, company_email_domain, p_cn.company_name_80_id) as tax_accnt_routing_domain_name80,
-- rule #4 
concat(tax_id_hash, bank_account_number_hash, bank_routing_number, company_email_domain, p_ad.address_80_id) as tax_accnt_routing_domain_addr80,
-- rule #5
concat(tax_id_hash, bank_account_number_hash, bank_routing_number, p_cn.company_name_80_id) as tax_accnt_routing_name80,
-- rule #6 
concat(tax_id_hash, p_cn.company_name_90_id, p_ad.address_80_id) as tax_name90_addr80,
-- rule #7
concat(tax_id_hash, company_email_domain, p_cn.company_name_90_id) as tax_domain_name90,
-- rule #8 
concat(tax_id_hash, company_email_domain, p_cn.company_name_80_id, p_ad.address_80_id) as tax_domain_name80_addr80,
-- rule #9 
concat(tax_id_hash, bank_account_number_hash, bank_routing_number, company_email_domain, p_cn.company_name_65_id, p_ad.address_65_id) as tax_accnt_routing_domain_name65_addr65,
-- rule #10 
concat(bank_account_number_hash, bank_routing_number, p_ad.address_80_id,  p_cn.company_name_90_id) as accnt_routing_addr80_name90, 
-- rule #11
concat(bank_account_number_hash, bank_routing_number, company_email_domain, p_cn.company_name_90_id) as accnt_routing_domain_name90,
-- rule #12 
concat(bank_account_number_hash, bank_routing_number, company_email_domain, p_ad.address_90_id, p_cn.company_name_65_id) as accnt_routing_domain_addr90_name65,
-- rule #13
concat(company_email_domain, p_ad.address_80_id, p_cn.company_name_80_id) as domain_addr80_name80, 
-- rule #14
concat(company_email_domain, p_ad.address_95_id, p_cn.company_name_65_id) as domain_addr95_name65,
--rule #15
concat(p_ad.address_95_id, p_cn.company_name_95_id) as addr95_name95
FROM (
select 
  base.*,     
  concat(bank_account_number_hash,bank_routing_number) as routing_hash_cluster,
  trim(concat(trfmd_address1, ' ', COALESCE(trfmd_address2, ''))) as address_cluster,
  concat(tax_id_hash, bank_routing_number) as tax_routing_cluster
from base
) b
left join 
prob_company_name p_cn 
on b.company_name = p_cn.company_name
left join 
prob_address p_ad
on b.address_cluster = p_ad.address
