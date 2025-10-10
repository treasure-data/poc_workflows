select table_name,
column_name
from tables_column_metadata 
where data_type='varchar'
AND NOT REGEXP_LIKE(column_name, '${table.exclude_cols}')