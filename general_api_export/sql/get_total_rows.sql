with T1 as (SELECT count(*) as total_rows from ${td_table})

SELECT total_rows , total_rows> ${container_limit} as bool_val , CAST(CEIL(total_rows/CAST(${container_limit} as DOUBLE))  as integer) as upper_limit from T1