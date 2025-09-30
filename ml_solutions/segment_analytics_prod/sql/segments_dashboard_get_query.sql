SELECT
CASE
WHEN journey_name IS NOT NULL THEN split(segment_query, 'segment_id')[1] || ' segment_id, ' || '''' || stage_name || '''' || ' AS segment_name, ' || CAST(stage_population AS VARCHAR) || ' AS segment_population FROM cdp_audience_' || CAST(ps_id AS VARCHAR) || '.customers WHERE cdp_customer_id IN (' || stage_rule || ')'
ELSE segment_query
END as segment_query
FROM ${project_prefix}_segment_profile_mapping_temp