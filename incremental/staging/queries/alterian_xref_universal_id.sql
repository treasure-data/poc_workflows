DROP TABLE IF EXISTS ${stg}_${sub}.${tbl};
CREATE TABLE ${stg}_${sub}.${tbl} AS
select *, COALESCE(TD_TIME_PARSE(dt), TD_TIME_PARSE(CAST(CURRENT_TIMESTAMP as VARCHAR))) as inc_unix from alterian_xref where source_name = 'universal_id'