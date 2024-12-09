WITH TOP AS (
SELECT *, 
time as time_unixtime
FROM ${prefix}canonical_id_source_key_stats_top
)

SELECT 
time,
time_unixtime,
from_table,
total_distinct,
${td.last_results.distinct_cols}
FROM TOP
