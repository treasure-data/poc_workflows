with T2 AS (
    SELECT 
      ${id_col},
       ${id_col}s,
      SUM(cnt) AS sum_cnt -- minhash
  from 
    ${blocking_table}_temp2
    GROUP BY
      1,2
    HAVING
      SUM(cnt) > CAST(round(${hashes}*${jaccard_similarity_threshold}) AS INTEGER) -- Jaccard similarity
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