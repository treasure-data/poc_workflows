with valid_ids as (
  select 
    canonical_id as id, 
    count(distinct id) 
    from cdp_unification_${sub}.individual_unification_id_lookup group by 1 
having count(distinct id) < 10
order by 2 desc 
),
valid_data as (
select * from cdp_unification_${sub}.unification_master where individual_unification_id in (select id from valid_ids where id is not null)
)
SELECT a.*,
-- b.household_unification_id
FROM valid_data a 

-- -- ** use for household / hierarchiy id if needed ** 

-- left join cdp_unification_cf.unification_household_master b
-- on a.individual_unification_id=b.individual_unification_id;