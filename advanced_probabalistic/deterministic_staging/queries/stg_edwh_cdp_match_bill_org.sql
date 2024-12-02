
with base as (
  select 
organization_id,
lower(organization_name) as organization_name,
hshtaxid,
duns_number,
routingnumber,
hshaccountnumber,
case
        when nullif(lower(trim("address1")), 'null') is null then null
        when nullif(lower(trim("address1")), '') is null then null
        else array_join((transform((split(lower(trim("address1")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_address1",
case
        when nullif(lower(trim("address2")), 'null') is null then null
        when nullif(lower(trim("address2")), '') is null then null
        else array_join((transform((split(lower(trim("address2")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_address2",
      case
        when nullif(lower(trim("address3")), 'null') is null then null
        when nullif(lower(trim("address3")), '') is null then null
        else array_join((transform((split(lower(trim("address3")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_address3",
      case
        when nullif(lower(trim("address4")), 'null') is null then null
        when nullif(lower(trim("address4")), '') is null then null
        else array_join((transform((split(lower(trim("address4")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_address4",
 case
        when nullif(lower(trim("addresscity")), 'null') is null then null
        when nullif(lower(trim("addresscity")), '') is null then null
        else array_join((transform((split(lower(trim("addresscity")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_addresscity"
    , case
        when nullif(lower(trim("addressstate")), 'null') is null then null
        when nullif(lower(trim("addressstate")), '') is null then null
        else upper(trim(addressstate))
      end   AS  "trfmd_addressstate"
    -- 
    , case
        when nullif(lower(trim("addresscountry")), 'null') is null then null
        when nullif(lower(trim("addresscountry")), '') is null then null
        else upper(trim(addresscountry))
      end   AS  "trfmd_addresscountry",
case
        when nullif(lower(trim("addresszip")), 'null') is null then null
        when nullif(lower(trim("addresszip")), '') is null then null
        else array_join((transform((split(lower(trim("addresszip")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_addresszip",     
lower(email_domain) as email_domain,
user_count,
time

from bill_source.edwh_cdp_match_bill_org), 

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

select 
b.*, 
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
concat(hshtaxid, hshaccountnumber, routingnumber, p_ad.address_70_id, trfmd_addresszip, p_cn.company_name_70_id) as tax_accnt_routing_addr70_zip_name70,
-- rule #3
concat(hshtaxid, hshaccountnumber, routingnumber, email_domain, p_cn.company_name_80_id) as tax_accnt_routing_domain_name80,
-- rule #4 
concat(hshtaxid, hshaccountnumber, routingnumber, email_domain, p_ad.address_80_id) as tax_accnt_routing_domain_addr80,
-- rule #5
concat(hshtaxid, hshaccountnumber, routingnumber, p_cn.company_name_80_id) as tax_accnt_routing_name80,
-- rule #6 
concat(hshtaxid, p_cn.company_name_90_id, p_ad.address_80_id) as tax_name90_addr80,
-- rule #7
concat(hshtaxid, email_domain, p_cn.company_name_90_id) as tax_domain_name90,
-- rule #8 
concat(hshtaxid, email_domain, p_cn.company_name_80_id, p_ad.address_80_id) as tax_domain_name80_addr80,
-- rule #9 
concat(hshtaxid, hshaccountnumber, routingnumber, email_domain, p_cn.company_name_65_id, p_ad.address_65_id) as tax_accnt_routing_domain_name65_addr65,
-- rule #10 
concat(hshaccountnumber, routingnumber, p_ad.address_80_id,  p_cn.company_name_90_id) as accnt_routing_addr80_name90, 
-- rule #11
concat(hshaccountnumber, routingnumber, email_domain, p_cn.company_name_90_id) as accnt_routing_domain_name90,
-- rule #12 
concat(hshaccountnumber, routingnumber, email_domain, p_ad.address_90_id, p_cn.company_name_65_id) as accnt_routing_domain_addr90_name65,
-- rule #13
concat(email_domain, p_ad.address_80_id, p_cn.company_name_80_id) as domain_addr80_name80, 
-- rule #14
concat(email_domain, p_ad.address_95_id, p_cn.company_name_65_id) as domain_addr95_name65,
--rule #15
concat(p_ad.address_95_id, p_cn.company_name_95_id) as addr95_name95
FROM 
(
select 
*, 
concat(hshaccountnumber,routingnumber) as routing_hash_cluster,
concat(hshtaxid, routingnumber) as tax_routing_cluster,
trim(concat(trfmd_address1, ' ', COALESCE(trfmd_address2, ''))) as address_cluster
from base 
) b
left join 
prob_company_name p_cn
on 
b.organization_name = p_cn.company_name
left join 
prob_address p_ad
on 
b.address_cluster = p_ad.address