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

-- scaled as (
--   select 
--     td_id,
--     feature,
--     rescale(value, min(value) over (partition by feature), max(value) over (partition by feature)) as minmax
--   from 
--     exploded
-- ),

scaled_table as (select
  ${id_col},
  collect_list(feature(feature, value)) as features
from
  exploded
group by
  ${id_col}),


---Generating minhash signatures: it generates a MinHash for each id_col. MinHash is a technique for quickly estimating how similar two sets are. The "-n" argument is the number of hashes and "-k" is the number of key groups to use in the MinHash calculation. The result is a hash that can be used to compare the similarity of different sets (or, in this case, feature sets for different id_cols).


user_hash_temp as (select  minhash( ${id_col},features, "-n ${hashes} -k ${keygroups}") as (clusterId, ${id_col}) from scaled_table
-- ORDER BY ${id_col}
),

---Select only clusterids that have more than 1 ${id_col} in common
valid_clusters as (
  SELECT clusterid, count(*) 
  FROM user_hash_temp
  GROUP BY 1
  HAVING COUNT(*) > 1
)
,
valid_ids as (
  SELECT ${id_col}
  from user_hash_temp
  where clusterid in (SELECT DISTINCT clusterid from valid_clusters)
  group by 1 
  having count(*)> cast(round(${hashes}*${jaccard_similarity_threshold}) as integer)
)
,
user_hash AS (
SELECT a.* FROM user_hash_temp a
inner join valid_clusters b
on a.clusterid=b.clusterid
WHERE  a.${id_col} in (select distinct ${id_col} from valid_ids) 
)

-- DIGDAG_INSERT_LINE
SELECT *, RANK() OVER (order BY clusterid) AS cluster_rank 
from user_hash