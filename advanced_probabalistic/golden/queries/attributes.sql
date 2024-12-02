WITH gc_attendance AS (
    SELECT
        'GC Attendance' AS source,
        individual_unification_id,
        CAST(TO_UNIXTIME(DATE_PARSE(date_key, '%Y%m%d')) AS int) AS transactiontime_unix,
        park_description AS brand,
        CASE WHEN reuse = 'true' THEN 1 ELSE 0 END AS is_cancelled,  
        CASE WHEN forced_redemption = 'true' THEN 1 ELSE 0 END AS is_returned,  
        tax_amount
    FROM enriched_gc_attendance
),
micros_sales AS (
    SELECT 
        individual_unification_id,
        CAST(TO_UNIXTIME(DATE_PARSE(pos_business_date_key, '%Y%m%d')) AS int) AS transactiontime_unix,
        SUM(sales_amount) AS sales_amount,
        SUM(taxes) AS tax_amount,
        SUM(sales_quantity) AS sales_quantity,
        SUM(discount_quantity) AS discount_quantity,
        SUM(discount_amount) AS discount_amount
    FROM enriched_micros_menuitemsales
    GROUP BY 
        individual_unification_id,TO_UNIXTIME(DATE_PARSE(pos_business_date_key, '%Y%m%d'))
),
gc_sales AS (
    SELECT
        individual_unification_id,
        park_description AS brand,
        CAST(TO_UNIXTIME(DATE_PARSE(date_key, '%Y%m%d')) AS int) AS transactiontime_unix,
        SUM(sales_amount) AS sales_amount,
        SUM(tax_amount) AS tax_amount,
        SUM(sales_quantity) AS sales_quantity
    FROM enriched_gc_sales
    GROUP BY 
        individual_unification_id,
        park_description,TO_UNIXTIME(DATE_PARSE(date_key, '%Y%m%d'))
),
DiscountsAndCosts AS (
    SELECT
        individual_unification_id,
        SUM(discount_amount) AS total_discount_amount,
        AVG(discount_amount) AS avg_discount_amount
    FROM enriched_micros_discountsales
    GROUP BY individual_unification_id
),
all_sales AS (
    SELECT 
        individual_unification_id, 
        transactiontime_unix, 
        SUM(sales_amount) AS amount, 
        COUNT(sales_amount) AS total_purchases, 
        brand, 
        SUM(tax_amount) AS tax_amount, 
        SUM(sales_quantity) AS total_qty,
        ROW_NUMBER() OVER (PARTITION BY individual_unification_id ORDER BY transactiontime_unix) as rn
    FROM (
        SELECT 
            individual_unification_id,
            '' AS brand,
            transactiontime_unix,
            sales_amount,
            tax_amount,
            sales_quantity
        FROM micros_sales
        UNION ALL
        SELECT     
            individual_unification_id,
            brand,
            transactiontime_unix,
            sales_amount,
            tax_amount,
            sales_quantity
        FROM gc_sales
    ) a
    GROUP BY 
        individual_unification_id,
        transactiontime_unix,
        brand
),
all_sales_w_ttr AS (
    SELECT *,
        DATE_DIFF('day', LAG(FROM_UNIXTIME(transactiontime_unix)) OVER (PARTITION BY individual_unification_id ORDER BY transactiontime_unix), FROM_UNIXTIME(transactiontime_unix)) AS days_between_transactions,
        CASE WHEN rn = 2 THEN transactiontime_unix ELSE null END AS second_purchase_date_unix 
    FROM all_sales
),
transactions_attributes AS (
    SELECT 
        all_sales_w_ttr.individual_unification_id,
        MAX(CASE WHEN transactiontime_unix IS NOT NULL THEN transactiontime_unix ELSE NULL END) AS last_purchase_date_unix,
        SUM(total_purchases) AS total_purchases,
        MIN(CASE WHEN transactiontime_unix IS NOT NULL THEN transactiontime_unix ELSE NULL END) AS first_purchase_date_unix,
        ROUND(AVG(days_between_transactions)) AS avg_days_between_transactions,

        -- AVERAGE NUMBER OF ITEMS
        AVG(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '30' day) AS integer) THEN total_qty ELSE NULL END) AS lani_l30d, -- L30D Average Num Items (30 days)
        AVG(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '3' month) AS integer) THEN total_qty ELSE NULL END) AS lani_l3m, -- L3M Average Num Items (3 months)
        AVG(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '6' month) AS integer) THEN total_qty ELSE NULL END) AS lani_l6m, -- L6M Average Num Items (6 months)
        AVG(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '12' month) AS integer) THEN total_qty ELSE NULL END) AS lani_l12m, -- L12M Average Num Items (12 months)
        AVG(CASE WHEN transactiontime_unix < TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '12' month) AS integer) 
            AND transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '24' month) AS integer) THEN total_qty ELSE NULL END) AS lani_ly12m, -- LY12M Average Num Items (12-to-24 months)
        AVG(CASE WHEN amount IS NOT NULL THEN total_qty ELSE NULL END) AS lani, -- Lifetime Average Num Items (Lifetime)

        -- AVERAGE ORDER VALUES
        AVG(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '30' day) AS integer) THEN amount ELSE NULL END) AS aov_l30d, -- L30D Average Order Value (30 days)
        AVG(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '3' month) AS integer) THEN amount ELSE NULL END) AS aov_l3m, -- L3M Average Order Value (3 months)
        AVG(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '6' month) AS integer) THEN amount ELSE NULL END) AS aov_l6m, -- L6M Average Order Value (6 months)
        AVG(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '12' month) AS integer) THEN amount ELSE NULL END) AS aov_l12m, -- L12M Average Order Value (12 months)
        AVG(CASE WHEN transactiontime_unix < TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '12' month) AS integer) 
            AND transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '24' month) AS integer) THEN amount ELSE NULL END) AS aov_ly12m, -- LY12M Average Order Value (12-to-24 months)
        AVG(CASE WHEN amount IS NOT NULL THEN amount ELSE NULL END) AS aov, -- Lifetime Average Order Value (Lifetime)

        -- AVERAGE PRICES
        AVG(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '30' day)) THEN amount / NULLIF(total_qty, 0) ELSE NULL END) AS avg_item_price_l30d,
        AVG(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '3' month)) THEN amount / NULLIF(total_qty, 0) ELSE NULL END) AS avg_item_price_l3m,
        AVG(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '6' month)) THEN amount / NULLIF(total_qty, 0) ELSE NULL END) AS avg_item_price_l6m,
        AVG(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '12' month)) THEN amount / NULLIF(total_qty, 0) ELSE NULL END) AS avg_item_price_l12m,
        AVG(CASE WHEN transactiontime_unix BETWEEN TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '2' year)) AND TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '1' year)) THEN amount / NULLIF(total_qty, 0) ELSE NULL END) AS avg_item_price_ly12m,
        AVG(amount / NULLIF(total_qty, 0)) AS avg_item_price_lifetime,

        -- Largest order value                      
        MAX(CASE 
            WHEN transactiontime_unix >= TO_UNIXTIME(DATE_ADD('day', -30, CURRENT_DATE)) AND is_cancelled = 0 AND is_returned = 0 
            THEN amount 
            ELSE NULL 
        END) AS largest_order_value_last_30_days,
        MAX(CASE 
            WHEN is_cancelled = 0 AND is_returned = 0 
            THEN amount 
            ELSE NULL 
        END) AS largest_order_value_lifetime,

        -- Multi_brand purchases
        CASE WHEN COUNT(DISTINCT brand) > 1 THEN 1 ELSE 0 END AS multi_brand_purchases, 

        -- Order frequency
        SUM(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '30' day) AS integer) THEN 1 ELSE NULL END) AS lof_l30days, -- L30D Order Frequency (30 days)
        SUM(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '3' month) AS integer) THEN 1 ELSE NULL END) AS lof_l3m, -- L3M Order Frequency (3 months)
        SUM(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '6' month) AS integer) THEN 1 ELSE NULL END) AS lof_l6m, -- L6M Order Frequency (6 months)
        SUM(CASE WHEN transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '12' month) AS integer) THEN 1 ELSE NULL END) AS lof_l12m, -- L12M Order Frequency (12 months)
        SUM(CASE WHEN transactiontime_unix < TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '12' month) AS integer) 
            AND transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '24' month) AS integer) THEN 1 ELSE NULL END) AS lof_ly12m,
        SUM(CASE WHEN amount IS NOT NULL THEN 1 ELSE NULL END) AS lof, -- Lifetime Order Frequency (Lifetime)

        -- Repeat within 365 days
        CASE 
            WHEN DATE_DIFF('day', FROM_UNIXTIME(MIN(transactiontime_unix)), FROM_UNIXTIME(MAX(transactiontime_unix))) <= 365 
            THEN 1 
            ELSE 0 
        END AS repeat_within_365_days,        

        -- Order returned quantity
        SUM(CASE WHEN is_returned = 1 THEN total_qty ELSE 0 END) AS order_returned_quantity,

        -- Order returned revenue
        SUM(CASE WHEN is_returned = 1 THEN amount ELSE 0 END) AS order_returned_revenue,

        -- Order Revenue intervals
        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '30' day)) THEN amount ELSE 0 END) AS order_revenue_l30d,
        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '3' month)) THEN amount ELSE 0 END) AS order_revenue_l3m,
        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '6' month)) THEN amount ELSE 0 END) AS order_revenue_l6m,
        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '12' month)) THEN amount ELSE 0 END) AS order_revenue_l12m,
         SUM(CASE WHEN transactiontime_unix < TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '12' month) AS integer) 
            AND transactiontime_unix >= TRY_CAST(TO_UNIXTIME(DATE_TRUNC('day', NOW()) - INTERVAL '24' month) AS integer) THEN 1 ELSE NULL END) AS order_revenue_ly12m, 
        SUM(amount) AS life_time_order_revenue,

        -- Total items intervals
        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '30' day)) THEN total_qty ELSE 0 END) AS lti_l30d, -- L30D Total Items (30 days)
        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '3' month)) THEN total_qty ELSE 0 END) AS lti_l3m, -- L3M Total Items (3 months)
        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '6' month)) THEN total_qty ELSE 0 END) AS lti_l6m, -- L6M Total Items (6 months)
        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '12' month)) THEN total_qty ELSE 0 END) AS lti_l12m, -- L12M Total Items (12 months)
        SUM(CASE WHEN transactiontime_unix < TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '12' month)) 
            AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '24' month)) THEN total_qty ELSE 0 END) AS lti_ly12m, -- LY12M Total Items (12-to-24 months)
        SUM(CASE WHEN amount IS NOT NULL THEN total_qty ELSE 0 END) AS lti ,

        SUM(tax_amount) AS tax_amount,
        AVG(total_qty) AS avg_number_of_items_lifetime,
        AVG(amount) AS avg_order_value_lifetime,
        MAX(amount) AS max_order_value_lifetime,
        SUM(amount) AS total_sales_amount, 
        SUM(total_qty) AS total_sales_qty,
        MIN(second_purchase_date_unix) AS second_purchase_date_unix       

    FROM all_sales_w_ttr
    LEFT JOIN (SELECT individual_unification_id, is_cancelled, is_returned FROM gc_attendance) gc_attendance ON all_sales_w_ttr.individual_unification_id = gc_attendance.individual_unification_id
    GROUP BY all_sales_w_ttr.individual_unification_id
),
one_and_done AS (
    SELECT
        individual_unification_id,
        COUNT(DISTINCT brand) AS total_brands,
        CASE 
            WHEN COUNT(DISTINCT brand) = 1 THEN 'Yes'
            ELSE 'No'
        END AS one_and_done
    FROM all_sales_w_ttr
    GROUP BY individual_unification_id
),
Preferred_brand_intervals AS (
    SELECT
        individual_unification_id, brand,
        (SELECT brand FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '30' day))
         GROUP BY brand
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS preferred_brand_interval_l30d,

        (SELECT brand FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '3' month))
         GROUP BY brand
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS preferred_brand_interval_l3m,

        (SELECT brand FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '6' month))
         GROUP BY brand
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS preferred_brand_interval_l6m,

        (SELECT brand FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '12' month))
         GROUP BY brand
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS preferred_brand_interval_l12m,

        (SELECT brand FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix < TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '12' month)) 
            AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '24' month))
         GROUP BY brand
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS preferred_brand_interval_ly12m,

        (SELECT brand FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id
         GROUP BY brand
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS preferred_brand_interval_lifetime

    FROM all_sales_w_ttr sd1
    GROUP BY individual_unification_id, brand
),

base_1 AS (
    SELECT DISTINCT individual_unification_id 
    FROM unification_master
),

purchase_brands AS (
    SELECT
        individual_unification_id, brand,
        -- Preferred Brand Intervals
        (SELECT COUNT(DISTINCT brand) AS brand_interval FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '30' day))
         ) AS brand_interval_l30d,

        (SELECT COUNT(DISTINCT brand) AS brand_interval FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '3' month))
         ) AS brand_interval_l3m,

        (SELECT COUNT(DISTINCT brand) AS brand_interval FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '6' month))
         ) AS brand_interval_l6m,

        (SELECT COUNT(DISTINCT brand) AS brand_interval FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '12' month))
         ) AS brand_interval_l12m,

        (SELECT COUNT(DISTINCT brand) AS brand_interval FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix < TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '12' month)) 
            AND transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '24' month))
        ) AS brand_interval_ly12m,

        (SELECT COUNT(DISTINCT brand) AS brand_interval FROM all_sales_w_ttr sd2
         WHERE sd2.individual_unification_id = sd1.individual_unification_id AND transactiontime_unix > 0
         ) AS brand_interval_lifetime,
         ROW_NUMBER() OVER (PARTITION BY individual_unification_id ORDER BY brand) AS rn
    FROM all_sales_w_ttr sd1
    GROUP BY individual_unification_id, brand
),
purchase_brands2 as (
  select * 
  from purchase_brands 
  where rn = 1 
),
last_brand_purchase AS (
    SELECT individual_unification_id, brand, transactiontime_unix
    FROM (
        SELECT 
            individual_unification_id, 
            brand, 
            transactiontime_unix,
            ROW_NUMBER() OVER (PARTITION BY individual_unification_id ORDER BY transactiontime_unix DESC) AS rn
        FROM all_sales_w_ttr
    ) sub
    WHERE sub.rn = 1
),

Purchases AS (
    SELECT 
        individual_unification_id, 
        transactiontime_unix,
        LEAD(transactiontime_unix) OVER (PARTITION BY individual_unification_id ORDER BY transactiontime_unix) AS next_purchase_time
    FROM all_sales_w_ttr
),

early_purchase_percentage AS (
    SELECT individual_unification_id,
        COUNT(*) AS total_customers,
        COUNT(CASE WHEN next_purchase_time IS NOT NULL AND (next_purchase_time - transactiontime_unix) <= 90 * 86400 THEN 1 END) AS early_repeat_purchasers,
        ROUND((100.0 * COUNT(CASE WHEN next_purchase_time IS NOT NULL AND (next_purchase_time - transactiontime_unix) <= 90 * 86400 THEN 1 END) / COUNT(*)), 2) AS early_repeat_purchase_percentage
    FROM Purchases 
    WHERE next_purchase_time IS NOT NULL
    GROUP BY individual_unification_id
),

purchase_interval AS (
    SELECT individual_unification_id,
        total_purchases,
        DATE_DIFF('day', FROM_UNIXTIME(last_purchase_date_unix), CURRENT_TIMESTAMP) AS time_since_last_purchase, -- Days since latest order
        DATE_DIFF('day', FROM_UNIXTIME(first_purchase_date_unix), FROM_UNIXTIME(last_purchase_date_unix)) AS first_to_latest_order -- First-to-latest order
    FROM transactions_attributes
    WHERE total_purchases > 2
),
-- OrderedPurchases AS (
--     SELECT 
--         individual_unification_id, 
--         transactiontime_unix,
--         LEAD(transactiontime_unix, 1) OVER (PARTITION BY individual_unification_id ORDER BY transactiontime_unix) AS second_purchase_time
--     FROM all_sales_w_ttr
-- ),
-- first_to_second_order AS (
--     SELECT 
--         individual_unification_id,
--         transactiontime_unix AS first_purchase_time,
--         second_purchase_time,
--         DATE_DIFF('day', FROM_UNIXTIME(transactiontime_unix), FROM_UNIXTIME(second_purchase_time)) AS first_to_second_order
--     FROM OrderedPurchases
--     WHERE second_purchase_time IS NOT NULL
-- ),
AdditionalMetrics AS (
    SELECT
        all_sales_w_ttr.individual_unification_id,

        SUM(CASE WHEN is_cancelled = 1 THEN total_qty ELSE 0 END) AS total_canceled_quantity,
        SUM(CASE WHEN is_cancelled = 1 THEN amount ELSE 0 END) AS total_canceled_revenue,

        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '30' day)) AND is_returned = 1 THEN total_qty ELSE 0 END) AS returned_quantity_l30d,
        SUM(CASE WHEN transactiontime_unix >= TO_UNIXTIME(DATE_TRUNC('day', NOW() - INTERVAL '30' day)) THEN amount ELSE 0 END) AS returned_revenue_l30d,

        MAX(amount) AS largest_list_price,
        SUM(total_qty) AS total_quantity,
        SUM(amount) AS total_revenue,

        MAX(transactiontime_unix) AS latest_order_date,
        MIN(transactiontime_unix) AS first_order_date,

        MIN(transactiontime_unix) AS first_purchase_date

    FROM all_sales_w_ttr
    LEFT JOIN (SELECT individual_unification_id, is_cancelled, is_returned FROM gc_attendance) gc_attendance ON all_sales_w_ttr.individual_unification_id = gc_attendance.individual_unification_id
    GROUP BY all_sales_w_ttr.individual_unification_id
),

purchase_average AS (
    SELECT individual_unification_id,
        AVG(first_to_latest_order/(total_purchases-1)) AS average_purchase,
        MAX(time_since_last_purchase) AS time_since_last_purchase,
        MAX(first_to_latest_order) AS purchase_period
    FROM purchase_interval
    GROUP BY individual_unification_id
),

final_results AS (
    SELECT 
        COALESCE(cd.individual_unification_id, 'no_individual_unification_id') AS individual_unification_id,
        COALESCE(ta.first_purchase_date_unix, NULL) AS first_purchase_date_unix,
        COALESCE(ta.last_purchase_date_unix, NULL) AS last_purchase_date_unix,
        COALESCE(ta.total_purchases, NULL) AS total_purchases,
        COALESCE(pa.average_purchase, NULL) AS average_purchase,
        COALESCE(ta.avg_days_between_transactions, NULL) AS avg_days_between_transactions,
        -- COALESCE(pa.time_since_last_purchase, NULL) AS time_since_last_purchase, -- Days since latest order
        COALESCE(DATE_DIFF('day', FROM_UNIXTIME(ta.last_purchase_date_unix), CURRENT_TIMESTAMP), NULL) AS time_since_last_purchase, -- Days since latest order

        -- Average number of items
        COALESCE(ta.lani_l30d, NULL) AS lani_l30d,
        COALESCE(ta.lani_l3m, NULL) AS lani_l3m,
        COALESCE(ta.lani_l6m, NULL) AS lani_l6m,
        COALESCE(ta.lani_l12m, NULL) AS lani_l12m,         
        COALESCE(ta.lani_ly12m, NULL) AS lani_ly12m, 
        COALESCE(ta.lani, NULL) AS lani, -- Lifetime Average Num Items (Lifetime)       

        -- Average order values
        COALESCE(ta.aov_l30d, NULL) AS aov_l30d,
        COALESCE(ta.aov_l3m, NULL) AS aov_l3m,
        COALESCE(ta.aov_l6m, NULL) AS aov_l6m,
        COALESCE(ta.aov_l12m, NULL) AS aov_l12m,         
        COALESCE(ta.aov_ly12m, NULL) AS aov_ly12m, 
        COALESCE(ta.aov, NULL) AS aov, -- Lifetime Average Order Value (Lifetime)

        -- Average prices
        COALESCE(ta.avg_item_price_l30d, NULL) AS avg_item_price_l30d,
        COALESCE(ta.avg_item_price_l3m, NULL) AS avg_item_price_l3m,
        COALESCE(ta.avg_item_price_l6m, NULL) AS avg_item_price_l6m,
        COALESCE(ta.avg_item_price_l12m, NULL) AS avg_item_price_l12m,         
        COALESCE(ta.avg_item_price_ly12m, NULL) AS avg_item_price_ly12m, 
        COALESCE(ta.avg_item_price_lifetime, NULL) AS avg_item_price_lifetime, 

        -- Brand intervals
        COALESCE(pb.brand_interval_l30d, NULL) AS brand_interval_l30d,         
        COALESCE(pb.brand_interval_l3m, NULL) AS brand_interval_l3m, 
        COALESCE(pb.brand_interval_l6m, NULL) AS brand_interval_l6m, 
        COALESCE(pb.brand_interval_l12m, NULL) AS brand_interval_l12m,         
        COALESCE(pb.brand_interval_ly12m, NULL) AS brand_interval_ly12m, 
        COALESCE(pb.brand_interval_lifetime, NULL) AS brand_interval_lifetime, 

        -- Total canceled quantity
        COALESCE(am.total_canceled_quantity, NULL) AS total_canceled_quantity,

        -- Total canceled revenue       
        COALESCE(am.total_canceled_revenue, NULL) AS total_canceled_revenue,

        -- Discount amount
        COALESCE(dac.total_discount_amount, NULL) AS total_discount_amount,

        -- Average discount percent
        COALESCE(dac.avg_discount_amount / NULLIF(dac.total_discount_amount, 0), 0) * 100 AS avg_discount_percent,

        -- Early repeat purchaser
        COALESCE(epp.early_repeat_purchase_percentage, NULL) AS early_repeat_purchase_percentage,

        -- First to latest order
        -- COALESCE(pi.first_to_latest_order, NULL) AS first_to_latest_order,
        COALESCE(DATE_DIFF('day', FROM_UNIXTIME(ta.first_purchase_date_unix), FROM_UNIXTIME(ta.last_purchase_date_unix)), NULL) AS first_to_latest_order, -- First-to-latest order
        -- First to second order
        -- COALESCE(ftso.first_to_second_order, NULL) AS first_to_second_order,
        COALESCE(DATE_DIFF('day', FROM_UNIXTIME(ta.first_purchase_date_unix), FROM_UNIXTIME(ta.second_purchase_date_unix)), NULL) AS first_to_second_order,
        -- Largest order value
        COALESCE(ta.largest_order_value_last_30_days, NULL) AS largest_order_value_last_30_days,
        COALESCE(ta.largest_order_value_lifetime, NULL) AS largest_order_value_lifetime,

        -- Multi-brand purchase
        COALESCE(ta.multi_brand_purchases, NULL) AS multi_brand_purchases,

        -- One_and_done
        COALESCE(od.one_and_done, NULL) AS one_and_done,

        -- Order frequency
        COALESCE(ta.lof_l30days, NULL) AS lof_l30days,
        COALESCE(ta.lof_l3m, NULL) AS lof_l3m,       
        COALESCE(ta.lof_l6m, NULL) AS lof_l6m,
        COALESCE(ta.lof_l12m, NULL) AS lof_l12m,
        COALESCE(ta.lof_ly12m, NULL) AS lof_ly12m,
        COALESCE(ta.lof, NULL) AS lof,

        -- Preferred brand intervals
        COALESCE(pbi.preferred_brand_interval_l30d, NULL) AS preferred_brand_interval_l30d,
        COALESCE(pbi.preferred_brand_interval_l3m, NULL) AS preferred_brand_interval_l3m,
        COALESCE(pbi.preferred_brand_interval_l6m, NULL) AS preferred_brand_interval_l6m,
        COALESCE(pbi.preferred_brand_interval_l12m, NULL) AS preferred_brand_interval_l12m,
        COALESCE(pbi.preferred_brand_interval_ly12m, NULL) AS preferred_brand_interval_ly12m,
        COALESCE(pbi.preferred_brand_interval_lifetime, NULL) AS preferred_brand_interval_lifetime,

        -- Repeat within 365 days
        COALESCE(ta.repeat_within_365_days, NULL) AS repeat_within_365_days,

        -- Order returned quantity
        COALESCE(ta.order_returned_quantity, NULL) AS order_returned_quantity,

        -- Order returned revenue
        COALESCE(ta.order_returned_revenue, NULL) AS order_returned_revenue,

        -- Order Revenue intervals
        COALESCE(ta.order_revenue_l30d, NULL) AS order_revenue_l30d,
        COALESCE(ta.order_revenue_l3m, NULL) AS order_revenue_l3m,
        COALESCE(ta.order_revenue_l6m, NULL) AS order_revenue_l6m,
        COALESCE(ta.order_revenue_l12m, NULL) AS order_revenue_l12m,
        COALESCE(ta.order_revenue_ly12m, NULL) AS order_revenue_ly12m,
        COALESCE(ta.life_time_order_revenue, NULL) AS life_time_order_revenue,

        -- Sum of items discounts
        COALESCE(dac.total_discount_amount, NULL) AS sum_of_items_discounts,      

        -- discount percent
       (dac.total_discount_amount * (-1) / NULLIF(ta.total_sales_amount, 0)) * 100 AS discount_percent, 
       
        -- Sum of tax_amounts   
        COALESCE(ta.tax_amount, NULL) AS tax_amounts,           

        -- Last brand purchase 
        COALESCE(lbp.brand, NULL) AS last_brand_purchase,  

        -- Total items
        COALESCE(ta.total_sales_qty, NULL) AS total_items,  

        -- Total items intervals
        COALESCE(ta.lti_l30d, NULL) AS lti_l30d,
        COALESCE(ta.lti_l3m, NULL) AS lti_l3m,
        COALESCE(ta.lti_l6m, NULL) AS lti_l6m,
        COALESCE(ta.lti_l12m, NULL) AS lti_l12m,
        COALESCE(ta.lti_ly12m, NULL) AS lti_ly12m,
        COALESCE(ta.lti, NULL) AS lti

    FROM base_1 cd
    -- LEFT JOIN all_sales_w_ttr all ON cd.individual_unification_id = all.individual_unification_id
    LEFT JOIN transactions_attributes ta ON cd.individual_unification_id = ta.individual_unification_id
    LEFT JOIN one_and_done od ON cd.individual_unification_id = od.individual_unification_id
    LEFT JOIN DiscountsAndCosts dac ON cd.individual_unification_id = dac.individual_unification_id
    LEFT JOIN Preferred_brand_intervals pbi ON cd.individual_unification_id = pbi.individual_unification_id
    LEFT JOIN last_brand_purchase lbp ON cd.individual_unification_id = lbp.individual_unification_id
    LEFT JOIN early_purchase_percentage epp ON cd.individual_unification_id = epp.individual_unification_id
    -- LEFT JOIN purchase_interval pi ON cd.individual_unification_id = pi.individual_unification_id
    -- LEFT JOIN first_to_second_order ftso ON cd.individual_unification_id = ftso.individual_unification_id
    LEFT JOIN AdditionalMetrics am ON cd.individual_unification_id = am.individual_unification_id
    LEFT JOIN purchase_average pa ON cd.individual_unification_id = pa.individual_unification_id
    LEFT JOIN purchase_brands2 pb ON cd.individual_unification_id = pb.individual_unification_id    
)

SELECT  *
FROM final_results
