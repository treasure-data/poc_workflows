WITH T1 AS (
SELECT
'${table_db}.${table_name}' as table_name,
'${col_name}' as column_name,
count(*) as total_rows,
count_if(${col_name} IS NULL) as null_cnt,
ROUND(CAST(count_if(${col_name} IS NULL) AS DOUBLE) / count(*), 3) as null_perc,
max(TRY(CAST(${col_name} as DOUBLE))) as max_value,
min(TRY(CAST(${col_name} as DOUBLE))) as  min_value,
ROUND(avg(TRY(CAST(${col_name} as DOUBLE))), 3) as average_value,
ROUND(STDDEV(TRY(CAST(${col_name} as DOUBLE))), 3) as std_dev,
ROUND(variance(TRY(CAST(${col_name} as DOUBLE))), 3) as var,
approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.25) as "q1" ,
approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.50) as "q2" ,
approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.75) as "q3" ,
ROUND(approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.75) + (approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.75) - approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.25))*1.5, 3) as outl_upper_rng,
ROUND(approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.25) - (approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.75) - approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.25))*1.5, 3) as outl_lower_rng,
ROUND(approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.75) - approx_percentile(TRY(CAST(${col_name} as DOUBLE)), 0.25), 3) as iqr,
ROUND(KURTOSIS(TRY(CAST(${col_name} as DOUBLE))), 3) as kurtosis_val, 
ROUND(skewness(TRY(CAST(${col_name} as DOUBLE))), 3) as skewness_val
FROM ${table_db}.${table_name}
-- Below takes care of infinity values
WHERE is_finite(TRY(CAST(${col_name} as DOUBLE))) OR ${col_name} IS NULL 
),
T2 AS (
SELECT
'${col_name}' as column_name,
count(*) as num_outliers
FROM ${table_db}.${table_name}
WHERE TRY(CAST(${col_name} as DOUBLE)) > (SELECT outl_upper_rng FROM T1)
OR TRY(CAST(${col_name} as DOUBLE)) < (SELECT outl_lower_rng FROM T1)
group by 1
)
SELECT T1.*, T2.num_outliers, ROUND((T2.num_outliers + 0.01) / T1.total_rows, 3) AS perc_outliers
FROM T1 LEFT JOIN T2
ON T1.column_name = T2.column_name