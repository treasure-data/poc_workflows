drop table if exists ids_digests_new;

-- Merge ids_updated and ids_digests create ids_digests_new
create table ids_digests_new as
select
  ${unification_id},
  chunk_index,
  coalesce(reduce(
    ids_updated.id_set, 0,
    (result, id) -> bitwise_xor(result, from_big_endian_64(xxhash64(to_utf8(id)))),
    result -> result
  ), ids_digests.id_set_xxhash64) as id_set_xxhash64
from ids_updated
full outer join ids_digests using (${unification_id}, chunk_index)