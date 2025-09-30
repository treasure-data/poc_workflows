SELECT time, ps_id, ps_id_v4, ps_name, ps_population, root_folder, v5_flag, folder_id, folder_name, segment_id, segment_name, segment_population, realtime, rule, 
segment_type, journey_name, stage_name, stage_id, 
CAST(IF(segment_type != 'journey', NULL, REGEXP_EXTRACT(stage_idx, '^[^.]+')) AS INTEGER) as stage_idx, 
CAST(IF(segment_type != 'journey', NULL, REGEXP_EXTRACT(stage_population, '^[^.]+')) AS INTEGER) as stage_population, 
stage_rule
FROM ${project_prefix}_ps_stats_temp