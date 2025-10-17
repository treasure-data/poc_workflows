drop table if exists attrs_digests_new;

-- Merge attrs_updated and attrs_digests create attrs_digests_new
create table attrs_digests_new as
select
  ${unification_id},
  coalesce(from_big_endian_64(xxhash64(to_utf8(attrs_updated.payload))), attrs_digests.payload_xxhash64) as payload_xxhash64
from attrs_updated
full outer join attrs_digests using (${unification_id})