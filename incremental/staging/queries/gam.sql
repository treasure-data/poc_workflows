DROP TABLE IF EXISTS ${stg}_${sub}.${tbl};
CREATE TABLE ${stg}_${sub}.${tbl} WITH (
  bucketed_on = ARRAY['inc_unix'],
  bucket_count = 512
) AS

WITH date_bounds AS (
    SELECT
        MIN(TD_TIME_PARSE(logdate)) AS min_unix,
        1729589481 AS max_unix
    FROM gam
)
SELECT
    *,
    COALESCE(
        TD_TIME_PARSE(logdate),
        CASE WHEN time = 1729589481 THEN 
            TD_TIME_PARSE(
                CAST(date_add(
                    'second',
                    CAST(RAND() * (max_unix - min_unix) as BIGINT),
                    FROM_UNIXTIME(min_unix)
                ) AS VARCHAR)
            ) ELSE null end,
        time
    ) AS inc_unix
FROM
    gam,
    (SELECT * FROM date_bounds) bounds;
