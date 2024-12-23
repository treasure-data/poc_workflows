WITH valid_clusters as (
  SELECT clusterid, count(*) 
  FROM ${blocking_table}_temp
  GROUP BY 1
  HAVING COUNT(*) > 1
),
valid_ids as (
  SELECT ${id_col}
  from ${blocking_table}_temp
  where clusterid in (SELECT DISTINCT clusterid from valid_clusters)
  group by 1 
  
  having count(*)> CAST(round(${hashes}*${jaccard_similarity_threshold}) AS INTEGER)
)
,

user_hash AS (
SELECT * FROM ${blocking_table}_temp 
WHERE  ${id_col} in (select distinct ${id_col} from valid_ids) 
and clusterid in (SELECT DISTINCT clusterid from valid_clusters)
),

-- ---clustering over similar signatures: The hash results are then used to cluster similar signatures together. This is done by joining on the clusterId and counting the number of identical MinHash results for different id_cols. This count is then compared to a threshold based on the Jaccard similarity (a measure of the similarity between two sets).

T2 AS (
    SELECT 
      L.${id_col},
      R.${id_col} AS ${id_col}s,
      COUNT(*) AS cnt -- minhash
    FROM
      user_hash l LEFT OUTER
    JOIN
      user_hash r
      ON   L.Clusterid = R.Clusterid
    WHERE
      L.${id_col} != R.${id_col}
    GROUP BY
      l.${id_col},
      r.${id_col}
    HAVING
      COUNT(*) > CAST(round(${hashes}*${jaccard_similarity_threshold}) AS INTEGER) -- Jaccard similarity
),
T3 AS (
SELECT ${id_col}, ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(${id_col}s) || ARRAY [${id_col}])) AS userid_set
FROM T2
GROUP BY 1
),

-- -- Blocks Creation: For each cluster (group of similar id_cols), a unique block_key is generated. These are sorted and unique user sets are associated with each block_key. below userid_set is the list of ids which would fall in same block and for that below I tried to generate the distinct block ids for each user_id sets
-- -- e.g:- block_key_1, [id1,id60,id5.....]
  T4 AS (
  SELECT 
  CAST(UUID() AS VARCHAR) AS block_key,
  unique_set.userid_set as userid_set
FROM ( SELECT DISTINCT userid_set FROM T3 ) unique_set
  ),

-- ---Exploding and Ranking: The lists of ids associated with each block_key are then "exploded" into separate rows. After this, a rank is assigned to each block_key and a row number (duplicate_count) for each id within a block.
-- --  below id list is exploded to rows from list
-- -- e.g
-- -- block_key1  id1
-- -- block_key1  id60 .... end so on

T5 AS (
SELECT
  T4.block_key,
  id
FROM
  T4
CROSS JOIN UNNEST(userid_set) AS t (id)
),
T6 AS (
  select T5.*, rank() over (order by T5.block_key) as rnk
  FROM T5
)
-- ---Join and Filter: Finally, it joins the result with the input_table using the id_col. It filters out rows where duplicate_count is 2 or more, meaning it's eliminating duplicate rows within each block. The final output is the original data from input_table with an additional block_key and rnk (rank) field added for each id_col.

SELECT 
  T6.block_key, org.* , T6.rnk from T6 left join ${source_db}.${input_table} org on T6.id=org.${id_col}