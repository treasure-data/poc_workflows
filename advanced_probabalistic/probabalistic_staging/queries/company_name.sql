
with company_name as (
(select  distinct lower(organization_name) as company_name from edwh_cdp_match_bill_org)
union
(select distinct lower(company_name) as company_name from edwh_cdp_match_divvy_company)
) 

select ROW_NUMBER() OVER() as index, company_name from company_name