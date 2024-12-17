SELECT 
   unification_id, 
   MAX_BY(cluster_id, avg_cluster_similarity) as final_cluster_id, 
   
FROM
  prob_final_cluster
GROUP BY  
  unification_id
WHERE
  avg_cluster_similarity >= 0.8
-- ORDER BY
--   avg_cluster_similarity,
--   cluster_id