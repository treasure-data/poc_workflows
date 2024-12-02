
select distinct ${canonical_id_name}
from cdp_unification_${unif_name}.${unif_src_tbl}_full a
where exists (select 1 from ${dest_db}.${unif_src_tbl}_inc b
                where
                  (
                    a.${key} = b.${key}
                    and nullif(a.${key}, '') is not null
                  )
              )
      and nullif(${canonical_id_name}, '') is not null
;

