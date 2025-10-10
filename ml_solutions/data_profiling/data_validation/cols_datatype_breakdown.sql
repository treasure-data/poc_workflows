select 
  count(*) as total_columns,
  count_if(data_type='varchar') as varchar_cols,
  count_if(data_type='bigint') as bigint_cols,
  count_if(data_type='double') as double_cols
from ${project_prefix}_tables_column_metadata