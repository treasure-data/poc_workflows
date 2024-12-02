
drop table if exists cdp_unification_${unif_name}.enriched_${dest_tbl};
-- set session join_distribution_type = 'PARTITIONED'

create table cdp_unification_${unif_name}.enriched_${dest_tbl}
 ${td.last_results.bucket_config}
as
select b.${canonical_id_name}, a.*
from ${dest_db}.${dest_tbl} a
left join cdp_unification_${unif_name}.${unif_src_tbl}_full b
  on a.join_key = b.join_key
;
