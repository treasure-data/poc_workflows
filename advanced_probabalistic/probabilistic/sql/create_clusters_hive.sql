---clustering over similar signatures: The hash results are then used to cluster similar signatures together. This is done by joining on the clusterId and counting the number of identical MinHash results for different id_cols. This count is then compared to a threshold based on the Jaccard similarity (a measure of the similarity between two sets).
WITH T2 as (SELECT 
  J.${id_col},
  SORT_ARRAY(array_append(Collect_set(${id_col}s),J.${id_col})) AS userid_set
FROM (
    SELECT 
      L.${id_col},
      R.${id_col} AS ${id_col}s,
      COUNT(*) AS cnt -- minhash
    FROM
      ${blocking_table}_temp l LEFT OUTER
    JOIN
      ${blocking_table}_temp r
      ON (
        L.Clusterid = R.Clusterid
      ) -- minhash join
    WHERE
      L.${id_col} != R.${id_col}
    GROUP BY
      l.${id_col},
      r.${id_col}
    HAVING
      cnt > int(${hashes}*${jaccard_similarity_threshold}) -- Jaccard similarity
  ) J
GROUP BY
  J.${id_col}
ORDER BY
  J.${id_col} ASC
  ),

-- Blocks Creation: For each cluster (group of similar id_cols), a unique block_key is generated. These are sorted and unique user sets are associated with each block_key. below userid_set is the list of ids which would fall in same block and for that below I tried to generate the distinct block ids for each user_id sets
-- e.g:- block_key_1, [id1,id60,id5.....]
dp as (
  SELECT 
 Distinct  UUID() AS block_key,
  unique_set.userid_set as userid_set
FROM ( SELECT DISTINCT userid_set FROM T2 ) unique_set
)
,
---Exploding and Ranking: The lists of ids associated with each block_key are then "exploded" into separate rows. After this, a rank is assigned to each block_key and a row number (duplicate_count) for each id within a block.
--  below id list is exploded to rows from list
-- e.g
-- block_key1  id1
-- block_key1  id60 .... end so on

t3 as (select distinct block_key,id from dp
 LATERAL VIEW explode(userid_set) itemTable AS id),


--  generate the rank on block key and ids ( THIS PART WE CAN OPTIMISE BECAUSE IT WAS ACCORDING TO INTIAL STRUCTURE BUT NOW WE HAVE OPTIMSED THE ABOVE QUERIES STRUCTURE)
t4 as (select *,rank() over (order by t3.block_key) as rnk,
ROW_NUMBER() over (partition By block_key,id order by block_key,id ) as duplicate_count 
from t3)

-- DIGDAG_INSERT_LINE

---Join and Filter: Finally, it joins the result with the input_table using the id_col. It filters out rows where duplicate_count is 2 or more, meaning it's eliminating duplicate rows within each block. The final output is the original data from input_table with an additional block_key and rnk (rank) field added for each id_col.

select  t4.block_key, org.* , t4.rnk from t4 left join ${source_db}.${input_table} org on t4.id=org.${id_col}
where t4.duplicate_count<2