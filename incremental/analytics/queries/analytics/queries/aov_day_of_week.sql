with micros_sales AS (
    SELECT 
        -- coalesce(individual_unification_id,  coalesce(global_id, 'no_id')) id,
        date_format(from_unixtime(CAST(transactiontime_unix AS int)), '%Y') AS season,
        month(from_unixtime(CAST(transactiontime_unix AS int))) AS month,
        DAY_OF_WEEK(from_unixtime(CAST(transactiontime_unix AS int))) AS weekday,
        SUM(sales_amount) AS sales_amount,
        SUM(sales_quantity) AS sales_quantity
    FROM enriched_micros_menuitemsales
    GROUP BY 1, 2, 3
),
gc_sales AS (
    SELECT
        -- coalesce(individual_unification_id, coalesce(global_id, 'no_id')) id,
        date_format(from_unixtime(CAST(TO_UNIXTIME(DATE_PARSE(date_key, '%Y%m%d')) AS int)), '%Y') AS season,
        month(DATE_PARSE(date_key, '%Y%m%d')) AS month,
        DAY_OF_WEEK(from_unixtime(CAST(TO_UNIXTIME(DATE_PARSE(date_key, '%Y%m%d')) AS int))) AS weekday,
        SUM(sales_amount) AS sales_amount,
        SUM(sales_quantity) AS sales_quantity
    FROM enriched_gc_sales
    GROUP BY 1, 2, 3 
)
select season, month, weekday, sum(sales_amount) aov, SUM(sales_quantity) sales_quantity
from (
  select * from micros_sales
  union all
  select * from gc_sales
) a
group by  season, month, weekday
order by season, month, weekday