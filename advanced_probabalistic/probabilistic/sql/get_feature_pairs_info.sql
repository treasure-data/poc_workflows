select 
  ${session_id} as session_id,
  '${td.each.column1}' as column_1, 
  '${td.each.column2}' as column_2, 
  ${td.each.column1} as column_1_value, 
  ${td.each.column2} as column_2_value, 
  count(*) as value_counts
from ${source_db}.${input_table}
group by 1, 2, 3, 4, 5
-- having count(*) > 5
order by 6 desc 
limit ${top_k_vals} 