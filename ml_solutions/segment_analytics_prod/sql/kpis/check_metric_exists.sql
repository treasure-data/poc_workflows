with cte as(
  select split(metric_names, ', ') AS  metric_names from ${project_prefix}_params_yml 
  where output_table = '${td.each.output_table}'
),
cte2 as( 
SELECT array_agg(distinct metric_name) as metric_names  FROM ${project_prefix}_final_lookup_table 
WHERE metric_table = '${td.each.output_table}'
), 
cte3 as(
select array_agg(distinct segment_id) as new_segment_ids from ${project_prefix}_ps_stats
),cte4 as(
select array_agg(distinct segment_id) as old_segment_ids from ${project_prefix}_final_lookup_table
)
select 
array_except((select metric_names from cte), (select metric_names from cte2) ) as new_metric_list,
(cardinality(array_except((select metric_names from cte), (select metric_names from cte2) ))) as new_metrics_count,
array_except((select new_segment_ids from cte3), (select old_segment_ids from cte4) ) as new_segments_list,
cardinality(array_except((select new_segment_ids from cte3), (select old_segment_ids from cte4) )) as new_segments_count,
(select new_segment_ids from cte3) as new_segment_ids,
(select old_segment_ids from cte4)  as old_segment_ids, '${td.each.output_table}' as metric_table