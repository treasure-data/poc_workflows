select ${td.each.column_string}, count(*) as value_count

from ${source_db}.${input_table}
group by ${td.each.column_string}
-- having count(*) > 5
order by count(*) desc 
limit ${top_k_vals} 