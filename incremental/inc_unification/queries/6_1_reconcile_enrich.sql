
-- Insert current unification result to previous non matching enriched table
Insert into cdp_unification_${unif_name}.${unif_src_tbl}_full
select * from cdp_unification_${unif_name}.enriched_${unif_src_tbl};
