SELECT 
   *
FROM
  prob_final_cluster
WHERE
  avg_cluster_similarity >= 0.8
ORDER BY
  avg_cluster_similarity,
  cluster_id