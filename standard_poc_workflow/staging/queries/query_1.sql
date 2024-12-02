with base as (
  select distinct
    "accnt_nmbr"
  , trim("cnsmr_typ_cd") cnsmr_typ_cd
  , trim("cnsmr_nm") cnsmr_nm
  , trim("cnsmr_addrss_1") cnsmr_addrss_1
  , trim("cnsmr_addrss_2") cnsmr_addrss_2
  , trim("addr_city") addr_city
  , trim("cnsmr_st") cnsmr_st
  , "cnsmr_zp"
  , trim("applcnt_geo_st") applcnt_geo_st
  , trim("geo_cnty") geo_cnty
  , trim("recorddate") recorddate
  --
  , case
      when nullif(lower(trim("cnsmr_nm")), 'null') is null then null
      when nullif(lower(trim("cnsmr_nm")), '') is null then null
      else array_join((transform((split(lower(trim("cnsmr_nm")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
    end   AS  "trfmd_name"
  --
    , case
      when nullif(lower(trim("cnsmr_addrss_1")), 'null') is null then null
      when nullif(lower(trim("cnsmr_addrss_1")), '') is null then null
      else array_join((transform((split(lower(trim("cnsmr_addrss_1")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
    end   AS  "trfmd_address1"
  --
    , case
      when nullif(lower(trim("cnsmr_addrss_2")), 'null') is null then null
      when nullif(lower(trim("cnsmr_addrss_2")), '') is null then null
      else array_join((transform((split(lower(trim("cnsmr_addrss_2")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
    end   AS  "trfmd_address2"
  , case
      when nullif(lower(trim("addr_city")), 'null') is null then null
      when nullif(lower(trim("addr_city")), '') is null then null
      else array_join((transform((split(lower(trim("addr_city")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
    end   AS  "trfmd_city"
  --
  , case
      when nullif(lower(trim("cnsmr_st")), 'null') is null then null
      when nullif(lower(trim("cnsmr_st")), '') is null then null
      else array_join((transform((split(lower(trim("cnsmr_st")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
    end   AS  "trfmd_state"
  --
  , trim(cast("cnsmr_zp" as VARCHAR)) "trfmd_postalcode"
  --
  , SUBSTRING(cast("cnsmr_zp" as VARCHAR), 1, 5)  trfmd_postalcode_std
  --
  , CAST(to_unixtime(date_parse("recorddate", '%Y-%m-%d %H:%i:%s.%f')) AS BIGINT) as "recorddate_unix"
  FROM bana_v_lse_cnsmr_ods
)
select  *
, concat("trfmd_address1", ' ',"trfmd_postalcode_std") as "address1_postcode_cluster"
from base