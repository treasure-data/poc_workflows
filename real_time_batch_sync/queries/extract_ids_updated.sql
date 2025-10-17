-- If ids_digests table doesn't exsit, create an empty one.
create table if not exists ids_digests as
select
  cast(null as varchar) as ${unification_id},
  cast(null as bigint) as chunk_index,
  cast(null as bigint) as id_set_xxhash64
limit 0;

drop table if exists ids_updated;

-- For each 50 ID chunks, extract ID array with key type prefix
-- and save them on ids_updated if hash digest of the ID array
-- does not exist in ids_digests table
create table ids_updated as
select
  ${unification_id},
  chunk_index,
  id_set
from (
  select
    ${unification_id},
    follower_index / 50 as chunk_index,
    array_agg(follower_id_expr) as id_set
  from (
    select
      canonical_id as ${unification_id},
      (row_number() over (partition by gr.leader_ns, gr.leader_id order by row(gr.leader_ns, gr.leader_id))) as follower_index,
      follower_ks.key_name || ':' || follower_id as follower_id_expr
    from cdp_unification_${unification_name}.${unification_id}_graph gr
    join cdp_unification_${unification_name}.${unification_id}_keys follower_ks on gr.follower_ns = follower_ks.key_type
    join cdp_unification_${unification_name}.${unification_id}_lookup lk on gr.leader_id = lk.id and gr.leader_ns = lk.id_key_type
  ) es
  group by 1, 2
) ids_all
where not exists (
  select * from ids_digests
  where ids_digests.${unification_id} = ids_all.${unification_id}
  and ids_digests.chunk_index = ids_all.chunk_index
  and reduce(
    ids_all.id_set, 0,
    (result, id) -> bitwise_xor(result, from_big_endian_64(xxhash64(to_utf8(id)))),
    result -> result
  ) = ids_digests.id_set_xxhash64
)