WITH T1 AS (
SELECT DISTINCT column_name, 
CONCAT(column_name, ' NOT IN (SELECT col_value FROM BASE WHERE column_name =', '''', column_name, '''',  ')') as filter_syntax
FROM prob_eda_input_columns WHERE time = (SELECT max(time) FROM prob_eda_input_columns)
)
SELECT ARRAY_JOIN(ARRAY_AGG(filter_syntax), CONCAT(' AND ',chr(10))) AS filter_syntax FROM T1