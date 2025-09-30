SELECT TD_TIME_STRING(time, 'd!') as run_date, ps_id, ps_population, segment_id, segment_population,
CAST(IF(segment_type != 'journey', NULL, REGEXP_EXTRACT(stage_population, '^[^.]+')) AS INTEGER) as stage_population
FROM ${project_prefix}_ps_stats_temp