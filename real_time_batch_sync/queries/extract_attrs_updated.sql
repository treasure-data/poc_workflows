-- If attrs_digests table doesn't exsit, create an empty one.
create table if not exists attrs_digests as
select
  cast(null as varchar) as ${unification_id},
  cast(null as bigint) as payload_xxhash64
limit 0;

drop table if exists attrs_updated;

-- Extract JSON-formatted attributes from the master_table
-- and save them on attrs_updated if hash digest of the JSON
-- does not exist in attrs_digests table
create table attrs_updated as
select
  ${unification_id},
  payload
from (
  select
    ${unification_id} ,
    '{' ||
      array_join(array[
        '"ltv":' || json_format(cast(ltv as json)),
        '"last_year_transaction_count":' || json_format(cast(last_year_transaction_count as json)),
        '"next_best_category":' || json_format(cast(next_best_category as json))
      ], ',') ||
    '}' as payload
  from cdp_unification_${unification_name}.${master_table_name}
) attrs_all
where not exists (
  select * from attrs_digests
  where attrs_digests.${unification_id}  = attrs_all.${unification_id} 
  and attrs_digests.payload_xxhash64 = from_big_endian_64(xxhash64(to_utf8(attrs_all.payload)))
)