---- canonical_id_result_key_stats_full is created to keep a log of all unification results key_stats in full mode even if unification is running incrementally.

---- below query is picked from task +result_key_stats -- Here SOURCE TABLE was canonical_id_graph but we did union of this SOURCE TABLE with canonical_id_graph_prev which will contain ONLY PREVIOUS LEADER IDS. later on, we will copy newly added leader_ids to canonical_id_graph_prev table which will Maintain canonical_id_graph_prev table for next INC run.

${sql_str_result_key_stats}
