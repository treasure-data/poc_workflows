DROP TABLE IF EXISTS ${stg}_${sub}.${tbl};
CREATE TABLE ${stg}_${sub}.${tbl} WITH (
  bucketed_on = ARRAY['inc_unix'],
  bucket_count = 512
) AS
WITH date_bounds AS (
    SELECT
        MIN(TD_TIME_PARSE(dt)) AS min_unix,
        1729589481 AS max_unix
    FROM sailthru_daily_stream_events
)
SELECT
    *,
    COALESCE(
        TD_TIME_PARSE(dt),
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
    sailthru_daily_stream_events,
    (SELECT * FROM date_bounds) bounds;


