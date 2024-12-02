WITH ranked_data AS (
  SELECT
    cluster_rank,
    RANK() OVER (ORDER BY cluster_rank) AS rnk,
    COUNT(*) OVER () AS total_rows
  FROM
    ${blocking_table}_temp
)
SELECT
  (rnk - 1) / (total_rows / ${num_block_splits}) + 1 AS range_num,
  MIN(cluster_rank) AS range_start,
  MAX(cluster_rank) AS range_end
FROM
  ranked_data
GROUP BY
  (rnk - 1) / (total_rows / ${num_block_splits})
ORDER BY
  range_num;
