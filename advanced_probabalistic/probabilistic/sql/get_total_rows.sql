-- with T1 as (SELECT count(*) as total_rows from ${blocking_table})

-- SELECT total_rows , total_rows> ${record_limit} as bool_val , CAST(CEIL(total_rows/CAST(${record_limit} as DOUBLE))  as integer) as upper_limit from T1
with T1 as (SELECT max(rnk) as total_rows from ${blocking_table})

SELECT total_rows , total_rows> ${record_limit} as bool_val , CAST(CEIL(total_rows/CAST(${record_limit} as DOUBLE))  as integer) as upper_limit from T1