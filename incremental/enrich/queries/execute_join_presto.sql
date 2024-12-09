-- set session join_distribution_type = 'PARTITIONED'
-- set session time_partitioning_range = 'none'
DROP TABLE IF EXISTS ${td.each.tbl}_tmp;
CREATE TABLE ${td.each.tbl}_tmp 
with (bucketed_on = array['${canonical_id}'], bucket_count = 512)  
as
${td.each.query}