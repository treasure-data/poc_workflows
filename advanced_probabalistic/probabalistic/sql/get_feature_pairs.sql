with columns as (
  select distinct column_name from prob_eda_input_columns_temp
  where time in (select max(time) from prob_eda_input_columns_temp)
)

SELECT a.column_name AS column1, b.column_name AS column2
FROM columns AS a
JOIN columns AS b
ON a.column_name < b.column_name;