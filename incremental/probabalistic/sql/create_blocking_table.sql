WITH T1 AS (
  
  SELECT ${id_col},${td.last_results.cleaning_query} from ${source_db}.${input_table}
),

T2 AS (SELECT ${id_col}, ${td.last_results.blocking_query} from T1)

SELECT distinct B.* from ${source_db}.${input_table} A LEFT JOIN T2 B ON A.${id_col} = B.${id_col}


--SELECT A.* , B.* from ${input_table} A LEFT JOIN T2 B ON A.${id_col} = B.${id_col}

