
with record_counts as (
SELECT

    max_date,
    stg_record_count, 
    stg_inc_record_count
FROM
    (SELECT COUNT(*) AS stg_record_count, null as max_stg_date FROM ${tbl}) t1
CROSS JOIN
    (SELECT MAX(stg_inc_record_count) AS stg_inc_record_count, max(inc_unix) as max_date FROM ${stg}_${sub}_inc.${tbl}) t2
)

select * from record_counts