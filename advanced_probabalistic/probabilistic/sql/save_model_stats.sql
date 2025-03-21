SELECT
${session_id} as session_id,

(SELECT ARRAY_JOIN(ARRAY_AGG(CONCAT(column_name, ': ', col_weight)), ', ') FROM prob_dedupe_blocking_schema) AS dedupe_col_params,
${hashes} as hashes,
${keygroups} as keygroups,
${jaccard_similarity_threshold} as jaccard_sim_threshold,
${convergence_threshold} as convergence_threshold,
${cluster_threshold} as cluster_threshold,
${avg_spend_per_user} as annual_marketing_spend_per_user,
ROUND((${avg_spend_per_user}/365.1), 2) as daily_marketing_spend_per_user,
ROUND((${avg_spend_per_user}/52.1), 2) as  weely_marketing_spend_per_user,
'${string_type}' as string_similarity_algo,
'${fill_missing}' as fill_missing,
COUNT(time) as raw_record_cnt,
(SELECT COUNT(time) from ${blocking_table}) as cluster_table_cnt,
(SELECT APPROX_DISTINCT(block_key) from ${blocking_table}) as num_blocks,
(SELECT COUNT(time) FROM ${output_table}) as final_cluster_table_cnt,
(SELECT COUNT(DISTINCT cluster_id) FROM ${output_table}) as num_final_clusters,
(SELECT COUNT(time) - COUNT(DISTINCT cluster_id) FROM ${output_table}) as deduped_ids, 
(SELECT COUNT(time) - COUNT(DISTINCT cluster_id)FROM ${output_table})*${avg_spend_per_user} as estimated_savings,
SUBSTR(CAST(current_timestamp AS VARCHAR), 1, 19) as run_date,
date_diff('minute', from_unixtime(${session_unixtime}),  current_timestamp) as run_duration_mins,
'${exclude_overmerging}' AS exclude_overmerging
FROM ${source_db}.${input_table}