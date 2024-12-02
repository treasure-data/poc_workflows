WITH BASE AS (
  SELECT * FROM prob_eda_input_columns WHERE time = (SELECT max(time) FROM prob_eda_input_columns)
)
SELECT * FROM ${source_db}.${input_table}
WHERE ${td.last_results.filter_syntax}