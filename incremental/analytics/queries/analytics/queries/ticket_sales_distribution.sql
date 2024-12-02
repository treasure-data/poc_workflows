SELECT
    -- coalesce(individual_unification_id, coalesce(global_id, 'no_id')) id,
    -- date_format(from_unixtime(CAST(TO_UNIXTIME(DATE_PARSE(date_key, '%Y%m%d')) AS int)), '%Y') AS season,
    CAST(TO_UNIXTIME(DATE_PARSE(date_key, '%Y%m%d')) AS int) as date,
    date_format(from_unixtime(CAST(TO_UNIXTIME(DATE_PARSE(date_key, '%Y%m%d')) AS int)), '%Y') AS season,
    park_name,
    park_description,
    count(coalesce(ticket_id, coalesce(individual_unification_id, coalesce(global_id, 'no_id')))) ticket_count,
    SUM(sales_amount) AS sales_amount,
    SUM(sales_quantity) AS sales_quantity
FROM enriched_gc_sales
GROUP BY 1,2,3,4 