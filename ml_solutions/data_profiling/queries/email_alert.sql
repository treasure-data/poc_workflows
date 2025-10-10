WITH T1 AS (
  SELECT table_name, ARRAY_JOIN(ARRAY_AGG(column_name || ' -> ' || issue_type), ', ') as columns_to_check
  FROM ${project_prefix}_issues_to_check
  GROUP BY 1
)
SELECT 
ARRAY_JOIN(ARRAY_AGG(table_name || ': ' || columns_to_check), CONCAT(', ', chr(10))) as error_msg
FROM T1