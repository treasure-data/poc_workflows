with divvy_address_base as (
  SELECT
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
      end   AS  "trfmd_address2"
FROM
    bill_source.edwh_cdp_match_divvy_company
),

bill_address_base as (
    SELECT
    address1,
    address2,
    case
        when nullif(lower(trim("address1")), 'null') is null then null
        when nullif(lower(trim("address1")), '') is null then null
        else array_join((transform((split(lower(trim("address1")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_address1",
    case
        when nullif(lower(trim("address2")), 'null') is null then null
        when nullif(lower(trim("address2")), '') is null then null
        else array_join((transform((split(lower(trim("address2")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
      end   AS  "trfmd_address2"
FROM
    bill_source.edwh_cdp_match_bill_org
), 

address as (
  SELECT * FROM (
  SELECT 
    trim(concat(trfmd_address1, ' ', COALESCE(trfmd_address2, ''))) as address from divvy_address_base
    union 
   SELECT trim(concat(trfmd_address1, ' ', COALESCE(trfmd_address2, ''))) as address from bill_address_base
  )
)
select ROW_NUMBER() OVER() as index, address from address