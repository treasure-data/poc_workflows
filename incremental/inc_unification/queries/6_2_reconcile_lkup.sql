
-- Insert current unification result to previous non matching lookup table
Insert into cdp_unification_${unif_name}.${canonical_id_name}_lookup_full
select * from cdp_unification_${unif_name}.${canonical_id_name}_lookup;
;
