DROP TABLE IF EXISTS ${td.each.tbl}_tmp;
CREATE TABLE ${td.each.tbl}_tmp  (fan_id varchar, rollup_id varchar)
with (bucketed_on = array[${canonical_id}], bucket_count = 512);