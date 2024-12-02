-- -- Blocks Creation: For each cluster (group of similar id_cols), a unique block_key is generated. These are sorted and unique user sets are associated with each block_key. below userid_set is the list of ids which would fall in same block and for that below I tried to generate the distinct block ids for each user_id sets
-- -- e.g:- block_key_1, [id1,id60,id5.....]
with  T4 AS (
  SELECT 
  CAST(UUID() AS VARCHAR) AS block_key,
  unique_set.userid_set as userid_set
FROM ( SELECT DISTINCT (ARRAY[CAST(${id_col} AS VARCHAR)] || userid_set) as userid_set FROM ${blocking_table}_temp3 ) unique_set
  )

select * from t4