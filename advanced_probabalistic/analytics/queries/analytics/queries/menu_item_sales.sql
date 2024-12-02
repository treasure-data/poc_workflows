SELECT 
    date_format(from_unixtime(CAST(transactiontime_unix AS int)), '%Y') AS season,
    month(from_unixtime(CAST(transactiontime_unix AS int))) AS month,
    pos_menu_item_name,
    SUM(sales_quantity) AS sales_quantity,
    SUM(sales_amount) AS sales_amount,
    ROW_NUMBER() OVER (PARTITION BY date_format(from_unixtime(CAST(transactiontime_unix AS int)), '%Y'), month(from_unixtime(CAST(transactiontime_unix AS int))) ORDER BY sum(sales_quantity) DESC) AS rnk
FROM enriched_micros_menuitemsales
where individual_unification_id is not null
GROUP BY 1, 2, 3
ORDER BY 1, 2