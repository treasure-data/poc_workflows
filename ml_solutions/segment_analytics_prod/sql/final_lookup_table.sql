with cte as(
select 
  segment_id,
  concat(metric_table,'_kpis') as metric_table,
  metric_name,
  max(event_date) as max_event_date 
from ${project_prefix}_final_metrics_table  
group by 1,2,3 
order by 1,2,3
)
select *, (select max(time)  from ${project_prefix}_kpis_combined)as max_time from cte