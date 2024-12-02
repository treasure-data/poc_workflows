DROP TABLE IF EXISTS ${stg}_${sub}.${tbl};
CREATE TABLE ${stg}_${sub}.${tbl} WITH (
  bucketed_on = ARRAY['inc_unix'],
  bucket_count = 512
) AS
WITH date_bounds AS (
    SELECT
        MIN(TD_TIME_PARSE(dt)) AS min_unix,
        MAX(TD_TIME_PARSE(dt)) AS max_unix
    FROM sailthru_daily_stream_events
)
SELECT
    *,
    COALESCE(
        TD_TIME_PARSE(dt),
        TD_TIME_PARSE(
            CAST(date_add(
                'second',
                CAST(RAND() * (max_unix - min_unix) as BIGINT),
                FROM_UNIXTIME(min_unix)
            ) AS VARCHAR)
        )
    ) AS inc_unix
FROM
    sailthru_daily_stream_events,
    (SELECT * FROM date_bounds) bounds;


