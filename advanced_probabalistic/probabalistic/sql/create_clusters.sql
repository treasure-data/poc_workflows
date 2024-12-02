-- @TD distribute_strategy: aggressive


---Feature Generation: It generates categorical features based on a certain column list and their corresponding column names from a database table, named prob_hash_table in the sink_database.
WITH T1 As (select
${id_col},
    CATEGORICAL_FEATURES(
      array(${td.last_results.column_list}), -- categorical feature names
      ${td.last_results.column_names}-- corresponding column names
    )as features
from
  ${sink_database}.prob_hash_table ) ,



----- Feature Scaling: After the categorical feature generation, it scales these features to normalize the range of independent variables or features of data. This process uses a min-max scaling technique that scales the range of values of features between 0 and 1.

exploded as (
  select 
    ${id_col}, 
    extract_feature(feature) as feature,
    extract_weight(feature) as value
  from 
    T1 
    LATERAL VIEW explode(features) exploded AS feature
), 

scaled as (
  select 
    ${id_col},
    feature,
    rescale(value, min(value) over (partition by feature), max(value) over (partition by feature)) as minmax
  from 
    exploded
),

scaled_table as (select
  ${id_col},
  collect_list(feature(feature, minmax)) as features
from
  scaled
group by
  ${id_col}),


---Generating minhash signatures: it generates a MinHash for each id_col. MinHash is a technique for quickly estimating how similar two sets are. The "-n" argument is the number of hashes and "-k" is the number of key groups to use in the MinHash calculation. The result is a hash that can be used to compare the similarity of different sets (or, in this case, feature sets for different id_cols).

user_hash_temp as (select  minhash( ${id_col},features, "-n ${hashes} -k ${keygroups}") as (clusterId, ${id_col}) from scaled_table
-- ORDER BY ${id_col}
),

valid_clusters as (
  SELECT clusterid, count(*) 
  FROM user_hash_temp
  GROUP BY 1
  HAVING count(*) > 1
)
,
valid_ids as (
  SELECT ${id_col}
  from user_hash_temp
  group by 1 
  having count(*)> CAST(${hashes}*${jaccard_similarity_threshold} AS INTEGER)
)
,

user_hash AS (
SELECT * FROM user_hash_temp WHERE clusterid IN (SELECT clusterid FROM valid_clusters) or ${id_col} in (select ${id_col} from valid_ids)
),

---clustering over similar signatures: The hash results are then used to cluster similar signatures together. This is done by joining on the clusterId and counting the number of identical MinHash results for different id_cols. This count is then compared to a threshold based on the Jaccard similarity (a measure of the similarity between two sets).
T2 as (SELECT 
  J.${id_col},
  SORT_ARRAY(array_append(Collect_set(${id_col}s),J.${id_col})) AS userid_set
FROM (
    SELECT 
      L.${id_col},
      R.${id_col} AS ${id_col}s,
      COUNT(*) AS cnt -- minhash
    FROM
      user_hash l LEFT OUTER
    JOIN
      user_hash r
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


